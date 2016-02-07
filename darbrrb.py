#!/usr/bin/python3
# darbrrb.py: dar-based blu-ray redundant backup.
# Copyright 2013, Jared Jennings <jjenning@fastmail.fm>.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# ---
#
# See usage function below for documentation, or run this script with no
# arguments.
#
# Imports are below the settings.

darrc_template = """
--min-digits={settings.digits}
--compression=bzip2
--slice {settings.slice_size}M
# make crypto block size larger to reduce 
# likelihood of duplicate ciphertext
--crypto-block 131072
# DO NOT specify the AES key here: this script is burned on every
# backup disc, in the clear
--key aes:
-v
create:
-E "python3 {progname} {progargs} _create %p %b %n %e %c"
"""

class Settings:
# vvvvvvvv    Below are variables for you to mess with    vvvvvvvvvvvvv

    burner_device = '/dev/null'

# SCRATCH_DIR must have (DATA_DISCS + PARITY_DISCS) * DISC_SIZE mebibytes free
# to run backup. SCRATCH_DIR must not exist when this script is run.
# SCRATCH_DIR must not be a subdirectory of the directory being backed up.
    scratch_dir = '/home/tmp/backup_scratch'

# The number of digits to use when numbering archive slices. If you might have
# more slices than 10 to the power of DIGITS, you need to make DIGITS bigger
# accordingly.
    digits = 4

# Each redundancy set is composed of (DATA_DISCS + PARITY_DISCS) discs.
# These are like hard disk shelves with RAID, but with discs instead.
    data_discs = 3
    parity_discs = 2

# How many slices should be on each disc? I/O errors caused by media
# decay can truncate a slice; PAR1 then can't use the whole file. So
# let's be slightly wasteful.
    slices_per_disc = 500

# How much space is on a disc?
    # BluRay
    ## disc_size = 23841   # MiB
    # DVD
    ## disc_size = 4482    # MiB
    # CD-R
    ## disc_size = 680     # MiB
    disc_size = 30 # MiB


# ^^^^^^^^    Above are variables for you to mess with    ^^^^^^^^^^^

    # Reserved space is used for the filesystem, a copy of this program, a
    # README, and the par file that lists redundancy files.
    # Starting from http://stackoverflow.com/questions/468117, observed ISO +
    # Rock Ridge + Joliet filesystem overhead at 362K + 1.7K per filename.
    reserve_space = 10   # MiB

    # calculated settings

    @property
    def slices_per_set(self):
        return self.data_discs * self.slices_per_disc

    @property
    def total_set_count(self):
        return self.data_discs + self.parity_discs

    @property
    def scratch_free_needed(self):
        return self.total_set_count * self.disc_size

    @property
    def number_format(self):
        return '{:0' + str(self.digits) + '}'

    @property
    def _slice_size_not_counting_par_overhead(self):
        return (self.disc_size - self.reserve_space) // self.slices_per_disc

    @property
    def slice_size(self):
        # Account for the overhead that par introduces when creating
        # redundancy data for a dar slice. This overhead is par file
        # headers, checksums and the like.
        #
        # This formula was determined by making a lot of par files
        # from sets of files containing random data. The sets were of
        # sizes range(1,34) + [55], and file sizes were 1MB, 4MB, 16MB
        # and 64MB. Overhead was calculated using parpackets.py, and
        # a curve fit to the data using R's nls function::
        #
        #   nls(y ~ a*exp(b*x)+c, start=list(a=233087,b=-.05,c=3))
        #
        # It was a bit surprising to find (using a 3d scatter plot,
        # just before this curve fitting was done) that overhead did
        # not appear to vary across input file sizes: only with the
        # number of files.
        par_overhead_bytes = 270519 + \
                         230648 * \
                             math.exp(-0.195 * self.slices_per_disc)
        # The rounding error we introduce here makes darbrrb a bad
        # idea for media smaller than CDs.
        par_overhead_mb = math.ceil(par_overhead_bytes / 1048576)
        return self._slice_size_not_counting_par_overhead - par_overhead_mb

    # -n switch turns this off
    actually_burn = True



from itertools import chain
import sys
import os
import shutil
import glob
import getopt
import subprocess
import tempfile
import contextlib
import unittest
import logging
import io
import re
import itertools
import random
import math
try:
    from unittest.mock import Mock, patch, sentinel, call
except ImportError:
    from mock import Mock, patch, sentinel, call


def usage(settings):
    print(""" 
This script makes compressed, encrypted backups with {s.slice_size} MiB \
slices striped
across sets of {s.total_set_count} {s.disc_size} MiB optical discs, \
each set containing {s.data_discs} data disc(s)
and {s.parity_discs} parity disc(s). It \
requires the following software (or later versions):
Python 3.2; mock 1.0 (included in Python 3.3); dar 2.4.8; parchive 1.1;
growisofs 7.1; genisoimage 1.1.11.

When backing up, the directory {s.scratch_dir!r} should have 
{s.scratch_free_needed} MiB of space free. \
When restoring, copy this script off of the optical
disc first; you'll need to switch optical discs during the backup.

Usage: python3 {progname} [-v] [-n] dar <dar parameters>

Dar parameters of note:
    Creating archive:   -c <archive basename> -R <dir with files to backup>
    Extracting archive: -x <archive basename>

See dar(1) about parameters you can give to dar. Don't get fancy: only use
the ones that tell dar which mode to operate in, and which files to
archive.  Otherwise this script will not form a complete record of how dar
was run.

The -v switch, before dar, means to be verbose and show the dar command
being executed and the darrc used. The -n switch, before dar, means don't
burn any discs: just make directories containing the files that would have
been burned.

""".format(s=settings, progname=sys.argv[0]),
        file=sys.stderr)


@contextlib.contextmanager
def working_directory(newcwd):
    oldcwd = os.getcwd()
    try:
        os.chdir(newcwd)
        yield
    finally:
        os.chdir(oldcwd)

class NotEnoughScratchSpace(Exception):
    pass

class ScratchAlreadyExists(Exception):
    pass

# This is a class not because it needs state, but because I didn't want to pass
# settings around all the time
class Darbrrb:
    def __init__(self, settings, progname, progopts=()):
        self.settings = settings
        self.progname = progname
        self.progopts = progopts
        self.log = logging.getLogger('darbrrb')

    def _run(self, *args):
        self.log.info('running command {!r}'.format(args))
        subprocess.check_call(args)

    @property
    def darrc_contents(self):
        progargs = []
        for o, v in self.progopts:
            if v:
                progargs.extend(o, v)
            else:
                progargs.append(o)
        return darrc_template.format(settings=self.settings,
                progname=os.path.join(self.settings.scratch_dir, 
                        os.path.basename(self.progname)),
                progargs=' '.join(progargs))

    @property
    def readme(self):
        return """

This disc is part of a backup made by darbrrb, a tool that wraps the dar disk
archiver and the parchive file verification and repair tool to produce backups
with redundancy, for greater resilience against data loss due to backup media
failures or losses.

The darbrrb.py script you find on this disc is a copy of the one used to make
the backup; some salient settings are written toward the top of this file.
darbrrb ran dar with this darrc:

# ----------------
{contents}
# ----------------

The backup is split into redundancy sets of {s.total_set_count} discs. Out of
each set, dar archive slices are striped across the {s.data_discs} data
disc(s); par files with parity data for the dar slices are striped across the
{s.parity_discs} disc(s). Each disc can store {s.disc_size} MiB of data, and
each slice is {s.slice_size} MiB in size.

You may need to do a bit of hunting, then, to find a given slice, but if some
data is irretrievable from one of the discs, or some discs are missing, you
should be able to piece the slices back together.

To restore some files: first, make a directory somewhere with at least
{s.scratch_free_needed} MiB free. Copy the first and last slice of the backup
into your directory. Do some more stuff.

""".format(s=self.settings, contents=self.darrc_contents)
    # FIXME



    def dar(self, *args):
        # Perhaps darrc files can be non-ascii, but we haven't got any
        # non-ascii arguments to give here, so we'll stay on the safe side.
        indented_contents = self.darrc_contents.replace('\n', '\n        ')
        with open(os.path.join(self.settings.scratch_dir, 'darrc'),
                  'w', encoding='ascii') as darrc_file:
            self.log.info("""Contents of {name} follow:
{indented}
""".format(name=darrc_file.name, indented=indented_contents))
            darrc_file.write(self.darrc_contents)
            darrc_file.flush()
            # causes of this working_directory:
            # 1. when dar makes files, it will make them in the scratch_dir
            # 2. when dar calls this script, the _create method below will have
            #    scratch_dir as its cwd.
            with working_directory(self.settings.scratch_dir):
                self._run('dar', *(args + ('-B', darrc_file.name)))

    def wait_for_empty_disc(self):
        # There are a hundred cooler ways to do this; in 2013, I don't know of
        # one that works on many distros and OSes, much less ten years from
        # now. But you'll probably still be able to press enter, some way.
        if self.settings.actually_burn:
            input("press enter when you have inserted an empty disc:")

    def disc_dir(self, disc):
        return '__disc{:04d}'.format(disc)

    def disc_dirs(self):
        return sorted(glob.glob('__disc*'))

    def disc_title(self, basename, dar_slice_number, number_in_set):
        set_number = math.floor((dar_slice_number - 1) /
                self.settings.slices_per_set)
        # Max ISO 9660 vol id length is 32. Leave room for numbers and 2 dashes.
        # +1: These numbers are 0-based, but we want the ones in the title 1-based.
        return "{}-{:04d}-{:03d}".format(basename[:(32-4-3-2)],
                set_number + 1, number_in_set + 1)

    def scratch_mib_free(self):
        s = os.statvfs(self.settings.scratch_dir)
        return s.f_bavail * s.f_frsize // 1048576

    def ensure_free_space(self):
        mib_free = self.scratch_mib_free()
        if mib_free < self.settings.scratch_free_needed:
            raise NotEnoughScratchSpace(self.settings.scratch_dir,
                    self.settings.scratch_free_needed, mib_free)

    def ensure_scratch(self):
        if os.path.exists(self.settings.scratch_dir):
            if not os.path.isdir(self.settings.scratch_dir):
                raise ScratchAlreadyExists()
        else:
            os.mkdir(self.settings.scratch_dir)
        for disc in range(1, self.settings.total_set_count + 1):
            os.mkdir(os.path.join(self.settings.scratch_dir,
                    self.disc_dir(disc)))
        self.ensure_free_space()
        # this is the copy of this program that dar will run
        shutil.copyfile(self.progname,
                os.path.join(self.settings.scratch_dir,
                        os.path.basename(self.progname)))

    def make_redundancy_files(self, basename, dar_files, max_number):
        nslices = len(dar_files)
        if nslices == 0:
            raise ValueError('no dar slices for parchive to operate on')
        min_number = max_number - nslices + 1
        parformat = "{{}}.{0}-{0}.par".format(self.settings.number_format)
        parfilename = parformat.format(basename, min_number, max_number)
        self._run(*(['parchive',
                     '-n{}'.format(self.settings.parity_discs),
                     'a', parfilename,
                ] + dar_files))
        return parfilename

    def burn(self, basename, number, i, d, happening):
        if self.settings.actually_burn:
            self._run('growisofs', '-Z', self.settings.burner_device,
                    '-R', '-J', '-V', self.disc_title(basename, number, i),
                    d)
        else:
            destination = os.path.join(self.settings.scratch_dir,
                    self.disc_title(basename, number, i))
            self.log.info('not actually burning: moving files from {} to ' \
                    '{}'.format(d, destination))
            os.mkdir(destination)
            for f in glob.glob(os.path.join(d, '*')):
                shutil.move(f, os.path.join(destination, os.path.basename(f)))

    def _create(self, dir, basename, number, extension, happening):
        number = int(number)
        # note: dar has caused this function to be called; dar's cwd is
        # SCRATCH_DIR, hence so is ours
        dar_files_here = sorted(glob.glob('*.dar'))
        if len(dar_files_here) >= self.settings.data_discs or \
                happening == 'last_slice':
            parfilename = self.make_redundancy_files(
                    basename, dar_files_here, number)
            par_volumes = [f for f in os.listdir()
                           if re.match(r'\.[pqr][0-9][0-9]$', f)]
            for d in self.disc_dirs():
                shutil.copyfile(parfilename, os.path.join(d, parfilename))
                with io.open(os.path.join(d, 'README.txt'), 'wt') as readme:
                    readme.write(self.readme)
                this_program = os.path.basename(self.progname)
                shutil.copyfile(this_program, os.path.join(d, this_program))
            data_dirs = itertools.cycle(self.disc_dir(i+1)
                    for i in range(self.settings.data_discs))
            redundancy_dirs = itertools.cycle(self.disc_dir(i+1)
                    for i in range(self.settings.data_discs,
                            self.settings.total_set_count))
            for f, d in itertools.chain(
                    zip(dar_files_here, data_dirs),
                    zip(par_volumes, redundancy_dirs)):
                shutil.move(f, d)
        dars_on_discs = len(glob.glob(
                os.path.join(self.disc_dir(1), '*.dar')))
        size_if_we_dont_burn = (dars_on_discs + 1) * \
                self.settings.slice_size + \
                self.settings.reserve_space
        if size_if_we_dont_burn > self.settings.disc_size or \
                happening == 'last_slice':
            for i, d in enumerate(self.disc_dirs()):
                self.log.info("burning from {}".format(d))
                self.wait_for_empty_disc()
                self.burn(basename, number, i, d, happening)
                for fn in glob.glob(os.path.join(d, '*')):
                    os.unlink(fn)


class UsesTempScratchDir(unittest.TestCase):
    def setUp(self):
        self.settings = Settings()
        tempdir = tempfile.mkdtemp('darbrrb_test')
        self.old_tempfile_tempdir = tempfile.tempdir
        tempfile.tempdir = tempdir
        self.settings.scratch_dir = tempdir
        self.log = logging.getLogger('test code')
        self.dars_created = []
        self.par_pxx_files_created = []
    
    def tearDown(self):
        shutil.rmtree(self.settings.scratch_dir)
        tempfile.tempdir = self.old_tempfile_tempdir

    def mkdirp_parents(self, *names):
        for name in names:
            dir, file = os.path.split(name)
            where = self.settings.scratch_dir
            for d in dir.split(os.path.sep):
                new = os.path.join(where, d)
                if not os.path.exists(new):
                    os.mkdir(new)
                where = new
    
    def touch(self, *filenames):
        self.mkdirp_parents(*filenames)
        for name in filenames:
            with open(name, 'wt') as f:
                print('*', file=f)

    def mkdirp(self, *dirnames):
        self.mkdirp_parents(*dirnames)
        for name in dirnames:
            if not os.path.exists(name):
                os.mkdir(name)

    @property
    def dar_filename_format(self):
        return '{{}}.{}.dar'.format(self.settings.number_format)

    def touch_dar_file(self, basename, n):
        filename = self.dar_filename_format.format(basename,n)
        self.dars_created.append(filename)
        self.touch(filename)

    def touch_dar_files(self, basename, min, max):
        self.touch(*(self.dar_filename_format.format(basename,n)
                for n in range(min, max+1)))

    @property
    def par_main_format(self):
        return '{{}}.{0}-{0}.par'.format(self.settings.number_format)

    @property
    def par_volume_format(self):
        return '{{}}.{0}-{0}.{{}}{{:02d}}'.format(self.settings.number_format)

    def touch_par_files(self, basename, min_, max_, count):
        self.touch(self.par_main_format.format(basename, min_, max_))
        possible_volume_letters = 'pqrstuvxwyz'
        for i in range(count):
            # name.p00, name.p01, ..., name.p99, name.q00, ...
            letter = possible_volume_letters[i // 100]
            number = i % 100
            pfn = self.par_volume_format.format(basename, min_, max_,
                                                letter, number)
            self.par_pxx_files_created.append(pfn)
            self.touch(pfn)


class TestWorkingDirectoryContextManager(unittest.TestCase):
    @patch('os.chdir')
    @patch('os.getcwd', return_value='/zart')
    def testWorkingDirectory(self, getcwd, chdir):
        with working_directory('/fnord'):
            pass
        chdir.assert_has_calls([call('/fnord'),
                                call('/zart')])


@patch('os.chdir')
@patch('os.getcwd', return_value='/zart')
@patch.object(Darbrrb, '_run')
class TestInvokeDar(UsesTempScratchDir):
    def setUp(self):
        super().setUp()
        self.d = Darbrrb(self.settings, __file__)

    def testInvokeDar(self, _run, getcwd, chdir):
        self.d.dar('-c', 'basename', '-R', '/home/bla/photos')
        self.d._run.assert_called_with(
                'dar', '-c', 'basename', '-R', '/home/bla/photos',
                '-B', os.path.join(self.settings.scratch_dir, 'darrc'))

    def testCurrentWorkingDirectory(self, _run, getcwd, chdir):
        self.d.dar('-c', 'basename', '-R', '/home/bla/photos')
        # we can only assume the chdir calls surround the _run
        chdir.assert_has_calls([
                call(self.settings.scratch_dir),
                call('/zart')])


@patch.object(Darbrrb, '_run')
@patch.object(Darbrrb, 'wait_for_empty_disc')
class TestDarbrrbFourPlusOne(UsesTempScratchDir):
    data_discs = 4
    parity_discs = 1
    slices_per_disc = 5
    pretend_free_space = (data_discs + parity_discs) * 25000

    def setUp(self):
        super().setUp()
        self.settings.data_discs = self.data_discs
        self.settings.parity_discs = self.parity_discs
        self.settings.slices_per_disc = self.slices_per_disc
        self.settings.burner_device = '/dev/zero'
        # in our tests, _create is called, as though dar were invoking this
        # script; when dar does that, it's with the scratch dir as the cwd,
        # as tested above
        with patch.object(Darbrrb, 'scratch_mib_free',
                return_value=self.pretend_free_space):
            self.d = Darbrrb(self.settings, __file__)
            self.d.ensure_scratch()
            self.cwd = os.getcwd()
            os.chdir(self.settings.scratch_dir)

    def tearDown(self):
        super().tearDown()
        os.chdir(self.cwd)

    def testFirstFileOfSet(self, wfed, _run):
        self.touch_dar_files('thing', 1,1)
        everything = list(os.walk(self.settings.scratch_dir))
        self.d._create('dir', 'thing', '1', 'dar', 'operating')
        everything2 = list(os.walk(self.settings.scratch_dir))
        self.assertEqual(everything, everything2)

    def testLastFileOfSet(self, wfed, _run):
        self.touch_dar_files('thing', 1,4)
        self.touch_par_files('thing', 1,4,1)
        self.d._create('dir', 'thing', '4', 'dar', 'operating')
        self.d._run.assert_any_call(
                'parchive', '-n1', 'a',
                'thing.0001-0004.par',
                'thing.0001.dar', 'thing.0002.dar',
                'thing.0003.dar', 'thing.0004.dar')
        self.assertEqual(self.d._run.call_count, 1)
        self.assertEqual(glob.glob('*.dar'), [])
            
    # the last set, our discs may not be full, but because we stripe files
    # across the whole set, they are likely all non-empty.
    def testLastDiscOfBackupNotEven(self, wfed, _run):
        self.touch_dar_files('thing', 13, 14)
        self.touch_par_files('thing', 13, 14, 1)
        self.d._create('dir', 'thing', '14', 'dar', 'last_slice')
        self.d._run.assert_any_call(
                'parchive', '-n1', 'a',
                'thing.0013-0014.par',
                'thing.0013.dar', 'thing.0014.dar')
        self.d._run.assert_has_calls([
            call('growisofs', '-Z', '/dev/zero', '-R', '-J',
                    '-V', 'thing-0001-001', '__disc0001'),
            call('growisofs', '-Z', '/dev/zero', '-R', '-J',
                    '-V', 'thing-0001-002', '__disc0002'),
            call('growisofs', '-Z', '/dev/zero', '-R', '-J',
                    '-V', 'thing-0001-003', '__disc0003'),
            call('growisofs', '-Z', '/dev/zero', '-R', '-J',
                    '-V', 'thing-0001-004', '__disc0004'),
            call('growisofs', '-Z', '/dev/zero', '-R', '-J',
                    '-V', 'thing-0001-005', '__disc0005'),
            ])
        self.assertEqual(self.d._run.call_count, 6)

class TestDiscTitle(unittest.TestCase):
    def setUp(self):
        self.settings = Settings()
        self.settings.digits = 4
        self.d = Darbrrb(self.settings, __file__)
        self.log = logging.getLogger(self.__class__.__name__)

    def testQuickCheck(self):
        passes = 0
        fails = 0
        tries = 100
        for qc in range(tries):
            self.settings.data_discs = random.randint(1,40)
            self.settings.parity_discs = random.randint(1,40)
            self.settings.slices_per_disc = random.randint(1,100)
            sps = self.settings.slices_per_set
            complete_sets = random.randint(1,10)
            slices_in_last_set = random.randint(1,self.settings.slices_per_disc)
            calls = []
            # sets are numbered starting with 1
            for set in range(1,complete_sets+1):
                calls.append((set, (set * sps), 'operating'))
            calls.append((set+1, ((set * sps) + 
                                  random.randint(1, sps)),
                            'last_slice'))
            for set, slice, happening in calls:
                for disc in range(self.settings.total_set_count):
                    should_name = 'fnord-%04d-%03d' % (set, disc+1)
                    is_name = self.d.disc_title('fnord', slice, disc)
                    self.assertEqual(is_name, should_name,
                            'with {s.data_discs} data discs, ' \
                            '{s.slices_per_disc} slices per disc, ' \
                            '{cs} complete sets, {sils} slices in last set, ' \
                            'on set {set}, slice {slice}, ' \
                            'happening {happening}, calls was {calls}, ' \
                            'disc title should be ' \
                            '{should_name}, but is {is_name}'.format(
                                s=self.settings,
                                cs=complete_sets, sils=slices_in_last_set,
                                set=set, slice=slice, happening=happening,
                                calls=calls,
                                should_name=should_name, is_name=is_name))

    def testFourPlusOne(self):
        self.settings.data_discs = 4
        self.settings.parity_discs = 1
        self.settings.slices_per_disc = 11
        for set, slice, happening in (
                (1, 44, 'operating'),
                (2, 88, 'operating'),
                (3, 100, 'last_slice')):
            for disc in range(self.settings.total_set_count):
                should_name = 'fnord-%04d-%03d' % (set, disc + 1)
                is_name = self.d.disc_title('fnord', slice, disc)

    def testThreePlusEight(self):
        self.settings.data_discs = 3
        self.settings.parity_discs = 8
        self.settings.slices_per_disc = 20
        for set, slice, happening in ((1, 60, 'operating'), (2, 98, 'last_slice')):
            for disc in range(self.settings.total_set_count):
                should_name = 'fnord-%04d-%03d' % (set, disc + 1)
                is_name = self.d.disc_title('fnord', slice, disc)
                self.assertEqual(is_name, should_name)


@patch.object(Darbrrb, '_run')
@patch.object(Darbrrb, 'wait_for_empty_disc')
class TestWholeBackup(UsesTempScratchDir):
    data_discs = 4
    parity_discs = 1
    slices_per_disc = 5
    pretend_free_space = (data_discs + parity_discs) * 25000

    def setUp(self):
        super().setUp()
        self.log = logging.getLogger('test code')
        self.settings.data_discs = self.data_discs
        self.settings.parity_discs = self.parity_discs
        self.settings.slices_per_disc = self.slices_per_disc
        # if disc_size is too low, the math doesn't work right and the
        # tests fail. i wonder if this makes the system under test
        # fail in the same way...
        self.settings.disc_size = 23841
        self.settings.burner_device = '/dev/zero'
        # in our tests, _create is called, as though dar were invoking this
        # script; when dar does that, it's with the scratch dir as the cwd,
        # as tested above
        with patch.object(Darbrrb, 'scratch_mib_free',
                return_value=self.pretend_free_space):
            self.d = Darbrrb(self.settings, __file__)
            self.d.ensure_scratch()
            self.cwd = os.getcwd()
            os.chdir(self.settings.scratch_dir)
        self.discs_burned = []
        self.dar_create_slices_count = 0 # see test methods

    def tearDown(self):
        super().tearDown()
        os.chdir(self.cwd)


    def mock_dar(self, *args):
        # set self.dar_create_slices_count before you run this method
        #
        # we assume here that dar is being called to create the archive
        # args goes like dar -c basename -R root_of_files_to_save
        dir = args[-1]
        basename = args[2]
        for slice in range(self.dar_create_slices_count-1):
            # slice is zero-based but dar is one-based
            self.touch_dar_file(basename, slice+1)
            self.d._create(dir, basename, str(slice+1), 'dar', 'operating')
        slice += 1
        self.touch_dar_file(basename, slice+1)
        self.d._create(dir, basename, str(slice+1), 'dar', 'last_slice')

    def mock_parchive(self, *args):
        # expected args: parchive -n{} c outfile infile infile...
        # -n is the number of redundancy files to make
        # outfile is named {basename}.{slicemin}-{slicemax}.par
        outfile = args[3]
        nswitch = args[1]
        bn, numbers, par = outfile.split('.')
        n1, n2 = map(int, numbers.split('-'))
        count = int(nswitch.lstrip('-n'))
        self.touch_par_files(bn, n1, n2, count)

    def mock_growisofs(self, *args):
        # expected args: growisofs .... dirname
        files = list(map(os.path.basename,
                sorted(glob.glob(os.path.join(args[-1], '*')))))
        # self.log.debug('burning disc with files {}'.format(files))
        self.discs_burned.append(files)

    def mock__run(self, *args):
        # self.log.debug('args are %r', args)
        if args[0] == 'dar':
            self.mock_dar(*args)
        elif args[0] == 'parchive':
            self.mock_parchive(*args)
        elif args[0] == 'growisofs':
            self.mock_growisofs(*args)
        else:
            raise Exception('unknown command run under test', args)

    def testWholeBackup(self, wfed, _run):
        dir = 'dir'
        bn = 'whole'
        sett = self.settings
        complete_redundancy_sets = 2
        _run.side_effect = self.mock__run
        # two whole redundancy sets
        # a couple of file sets, not enough to make a redundancy set
        # a couple of odd files
        self.dar_create_slices_count = \
                sett.slices_per_disc * sett.data_discs * \
                        complete_redundancy_sets + \
                sett.slices_per_disc * sett.data_discs // 2 + \
                sett.data_discs // 2
        # for each complete or partial (at end) set of {data_discs} dar files,
        # we run par2 once
        expected_pars_run = sett.slices_per_disc * \
                        complete_redundancy_sets + \
                sett.slices_per_disc // 2 + \
                1
        # --- run code
        self.d.dar('-c', bn, '-R', '/fnord')
        max_slice = self.dar_create_slices_count
        # --- assertions
        #self.log.debug('calls:')
        #for c in self.d._run.call_args_list:
        #    self.log.debug('%r', c)
        def calls_running(executable):
            return list(x for x in self.d._run.call_args_list 
                    if x[0][0] == executable)
        self.assertEqual(len(calls_running('parchive')), expected_pars_run)
        expected_discs_burned = sett.total_set_count * (
            complete_redundancy_sets + 1)
        self.assertEqual(len(calls_running('growisofs')), 
                expected_discs_burned)
        unburned_dars = set(self.dars_created)
        unburned_par_volumes = set(self.par_pxx_files_created)
        for b in self.discs_burned:
            unburned_dars -= set(b)
            unburned_par_volumes -= set(b)
        self.assertEqual(len(unburned_dars), 0)
        return
        # the last redundancy set will be incomplete
        disc = 0
        for redundancy_set in range(complete_redundancy_sets + 1):
            for disc_in_set in range(sett.total_set_count):
                b = discs_burned[disc]
                self.assertTrue('README.txt' in b)
                self.assertTrue('darbrrb.py' in b)
                dars_on_b = [x for x in b if x.endswith('.dar')]
                par_volumes_on_b = [x for x in b if '.vol' in x 
                                 and x.endswith('.par2')]
                par_files_on_b = [x for x in b if '.vol' not in x
                                  and x.endswith('.par2')]
                if disc_in_set < sett.data_discs:
                    self.assertEqual(len(par_volumes_on_b), 0)
                else:
                    self.assertEqual(len(dars_on_b), 0)
                disc += 1
            

# the decorators above have made it so that TestWholeBackup is decorated. when
# we derive from it here, our ThreePlusEight class is already decorated; so we
# needn't (and mustn't) write the decorators again.
class TestWholeBackupThreePlusEight(TestWholeBackup):
    data_discs = 3
    parity_discs = 8
    slices_per_disc = 13 
    pretend_free_space = (data_discs + parity_discs) * 25000

# This is just to make sure all those integer divisions and multiplications
# aren't just happening to be right.
class TestWholeBackupNineteenPlusSeven(TestWholeBackup):
    data_discs = 19
    parity_discs = 7
    slices_per_disc = 31
    pretend_free_space = (data_discs + parity_discs) * 25000


if __name__ == '__main__':
    s = Settings()
    if len(sys.argv) < 2:
        usage(s)
        sys.exit(1)
    opts, remaining = getopt.getopt(sys.argv[1:], 'hvnt', ['help'])
    loglevel = logging.WARNING
    testing = False
    for o, v in opts:
        if o == '-h' or o == '--help':
            usage(s)
            sys.exit(1)
        elif o == '-v':
            loglevel -= 10
        elif o == '-n':
            s.actually_burn = False
        elif o == '-t':
            loglevel = logging.DEBUG
            testing = True
        else:
            raise Exception('unknown switch {}'.format(o))
    # style only influences how format is interpreted, not also how values are
    # interpolated into log messages. source: Python 3.2
    # logging/__init__.py:317, LogRecord class, getMessage method.
    logging.basicConfig(style='{', format='{name}: {message}',
            level=loglevel, stream=sys.stderr)
    if testing:
        sys.argv = [sys.argv[0]] + remaining
        sys.exit(unittest.main())
    d = Darbrrb(s, __file__, opts)
    if remaining[0] == 'dar':
        d.ensure_scratch()
        d.dar(*remaining[1:])
    elif remaining[0] == '_create':
        d._create(*remaining[1:])


