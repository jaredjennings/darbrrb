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
# imports are below

class Settings:
# vvvvvvvv    Below are variables for you to mess with    vvvvvvvvvvvvv

    burner_device = '/dev/null'

# SCRATCH_DIR must have (DATA_DISCS + PARITY_DISCS) * DISC_SIZE mebibytes free
# to run backup. SCRATCH_DIR must not exist when this script is run.
# SCRATCH_DIR must not be a subdirectory of the directory being backed up.
    scratch_dir = "scratch"

# The number of digits to use when numbering archive slices. If you might have
# more slices than 10 to the power of DIGITS, you need to make DIGITS bigger
# accordingly.
    digits = 4

# How much space is on a disc?
    disc_size = 4   # MiB

# How big should each slice of the archive be? You should set this to a value
# between DISC_SIZE / 100 and DISC_SIZE / 10.
    slice_size = 1  # MiB

# Each redundancy set is composed of (DATA_DISCS + PARITY_DISCS) discs.
# These are like hard disk shelves with RAID, but with discs instead.
    data_discs = 4
    parity_discs = 1

# ^^^^^^^^    Above are variables for you to mess with    ^^^^^^^^^^^

    @property
    def total_set_count(self):
        return self.data_discs + self.parity_discs

    @property
    def scratch_free_needed(self):
        return self.total_set_count * self.disc_size

    @property
    def number_format(self):
        return '{:0' + str(self.digits) + '}'

    # -v switch turns this on
    verbose = False
    # -n switch turns this on
    dry_run = False



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
import io
try:
    from unittest.mock import Mock, patch, sentinel, call
except ImportError:
    from mock import Mock, patch, sentinel, call


def usage(settings):
    print(""" 
This script wraps dar and par2 in such a way to make compressed, encrypted
backups with {s.slice_size} MiB slices striped across sets of {s.total_set_count} {s.disc_size} MiB optical discs,
each set containing {s.data_discs} data disc(s) and {s.parity_discs} parity disc(s).

When backing up, the directory {s.scratch_dir!r} should have {s.scratch_free_needed} MiB of space free.
When restoring, copy this script off of the optical disc first; you'll need to
switch optical discs during the backup.

Usage: python3 {progname} [-v] [-n] dar <dar parameters>

Dar parameters of note:
    Creating archive:   -c <archive basename> -R <dir with files to backup>
    Extracting archive: -x <archive basename>

See dar(1) about parameters you can give to dar. Don't get fancy: only use the
ones that tell dar which mode to operate in, and which files to archive.
Otherwise this script will not form a complete record of how dar was run.

The -v switch, before dar, means to be verbose and show the dar command being
executed and the darrc used. The -n switch, before dar, means dry run or
no-act: don't actually execute anything.

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
    def __init__(self, settings, progname):
        self.settings = settings
        self.progname = progname

    def _run(self, *args):
        if self.settings.verbose:
            print("command {0!r}".format(args))
        if self.settings.dry_run:
            pass
        else:
            subprocess.check_call(args)

    @property
    def darrc_contents(self):
        return """

--min-digits={settings.digits}
# --compression=bzip2
--slice {settings.slice_size}M
# make crypto block size larger to reduce likelihood of duplicate ciphertext
# --crypto-block 131072
# --key aes:
-v

create:
    -E "python3 {progname} _create %p %b %n %e %c"

""".format(settings=self.settings, progname=self.progname)

    @property
    def readme(self):
        return """

This disc is part of a backup made by darbrrb, a tool that wraps the dar disk
archiver and the par2 file verification and repair tool to produce backups
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
disc(s); par2 files with parity data for the dar slices are striped across the
{s.parity_discs} disc(s). Each disc can store {s.disc_size} MiB of data, and
each slice is {s.slice_size} MiB in size.

You may need to do a bit of hunting, then, to find a given slice, but if some
data is irretrievable from one of the discs, or some discs are missing, you
should be able to piece the slices back together.

""".format(s=self.settings, contents=self.darrc_contents)



    def dar(self, *args):
        # Perhaps darrc files can be non-ascii, but we haven't got any
        # non-ascii arguments to give here, so we'll stay on the safe side.
        with tempfile.NamedTemporaryFile('w', encoding='ascii') as darrc_file:
            if self.settings.verbose:
                print("""
vvvvvvvv Contents of darrc file {name}:     vvvvvvvvvvv
{contents}
^^^^^^^^ End contents of darrc file {name}. ^^^^^^^^^^^
""".format(name=darrc_file.name, contents=self.darrc_contents))
            darrc_file.write(self.darrc_contents)
            darrc_file.flush()
            # causes of this working_directory:
            # 1. when dar makes files, it will make them in the scratch_dir
            # 2. when dar calls this script, the _create method below will have
            #    scratch_dir as its cwd.
            with working_directory(self.settings.scratch_dir):
                self._run('dar', *(args + ('-B', darrc_file.name)))

    def wait_for_empty_disc(self):
        # stub
        input("press enter when you have inserted an empty disc:")

    def disc_dir(self, disc):
        return '__disc{}'.format(disc)

    def burn_and_remove_all(self):
        for disc in range(1, self.settings.total_set_count+1):
            disc_dir = self.disc_dir(disc)
            print("burning from {}".format(disc_dir))
            self.wait_for_empty_disc()
            self._run('growisofs', '-Z', self.settings.burner_device,
                    '-R', '-J', disc_dir)
            for fn in glob.glob(os.path.join(disc_dir, '*')):
                os.unlink(fn)

    def ensure_free_space(self):
        s = os.statvfs(self.settings.scratch_dir)
        mib_free = s.f_bavail * s.f_frsize // 1048576
        if mib_free < self.settings.scratch_free_needed:
            raise NotEnoughScratchSpace(self.settings.scratch_dir,
                    self.settings.scratch_free_needed, mib_free)

    def ensure_scratch(self):
        if os.path.exists(self.settings.scratch_dir):
            raise ScratchAlreadyExists()
        os.mkdir(self.settings.scratch_dir)
        for disc in range(1, self.settings.total_set_count + 1):
            os.mkdir(os.path.join(self.settings.scratch_dir,
                    self.disc_dir(disc)))
        ensure_free_space(self.settings)
        # this is the copy of this program that dar will run
        shutil.copyfile(self.progname, self.settings.scratch_dir)

    def make_redundancy_files(self, basename, dar_files, max_number):
        nslices = len(dar_files)
        min_number = max_number - nslices + 1
        par2format = "{{}}.{0}-{0}.par2".format(self.settings.number_format)
        par2filename = par2format.format(basename, min_number, max_number)
        redundancy_percent = 100 * self.settings.parity_discs // nslices
        self._run(*(['par2', 'c',
                '-n{}'.format(self.settings.parity_discs),
                '-u', '-r{}'.format(redundancy_percent),
                par2filename,
                ] + dar_files))
        return par2filename

    def _create(self, dir, basename, number, extension, happening):
        number = int(number)
        # note: dar has caused this function to be called; dar's cwd is
        # SCRATCH_DIR, hence so is ours
        dar_files = glob.glob('*.dar')
        par_volumes = glob.glob('*.vol*.par2')

        if len(dar_files) >= self.settings.data_discs or \
                happening == 'last_slice':
            par2filename = self.make_redundancy_files(
                    basename, dar_files, number)
            files_to_distribute = dar_files + par_volumes
            dirs = glob.glob(self.disc_dir('*'))
            if len(files_to_distribute) != len(dirs):
                print("I have {} files to distribute across {} dirs.".format(
                        len(files_to_distribute), len(dirs)), file=sys.stderr)
                print("I feel funny. Why is this happening to me?",
                        file=sys.stderr)
            for f, d in zip(files_to_distribute, dirs):
                shutil.copyfile(par2filename, d)
                shutil.move(f, d)
                with io.open(os.path.join(d, 'README.txt'), 'wt') as readme:
                    readme.write(self.readme)
            self.burn_and_remove_all()




# we can't use a usual mock for this, because mocks have a name
# attribute that means something else besides what a
# NamedTemporaryFile's name attribute means, and we can't mock that
# attribute.
class FunTempFile:
    name = 'zobefuosfewboeuf'
    def __init__(self, *args, **kwargs):
        pass
    def __enter__(self, *args, **kwargs):
        return self
    def __exit__(self, *args, **kwargs):
        pass
    def write(self, *args, **kwargs):
        pass
    def flush(self, *args, **kwargs):
        pass


class TestDarbrrbFourPlusOne(unittest.TestCase):
    def setUp(self):
        self.settings = Settings()
        self.settings.data_discs = 4
        self.settings.parity_discs = 1
        self.settings.burner_device = '/dev/zero'
        self.d = Darbrrb(self.settings, 'fnord')
        self.d._run = Mock()
        super().setUp()

    def testInvokeDar(self):
        self.d.settings.scratch_dir = sentinel.scratch_dir
        with patch('os.chdir') as chdir, \
                patch('os.getcwd', return_value=sentinel.oldcwd) as getcwd, \
                patch('tempfile.NamedTemporaryFile', new=FunTempFile) as ntf:
            self.d.dar('-c', 'basename', '-R', '/home/bla/photos')
            chdir.assert_has_calls([call(sentinel.scratch_dir),
                    call(sentinel.oldcwd)])
            self.d._run.assert_called_with(
                    'dar', '-c', 'basename', '-R', '/home/bla/photos',
                    '-B', FunTempFile.name)

    def testFirstDiscOfSet(self):
        def mock_glob(arg):
            if arg == '*.dar':
                return ['thing.0001.dar']
            else:
                return []
        with patch('shutil.move') as move, \
                patch('shutil.copyfile') as copyfile, \
                patch('os.unlink') as unlink, \
                patch('glob.glob', side_effect=mock_glob) as glob:
            self.d._create('dir', 'thing', '1', 'dar', 'operating')
            self.assertFalse(move.called)
            self.assertFalse(copyfile.called)
            self.assertFalse(unlink.called)
            self.assertFalse(self.d._run.called)

    def testLastDiscOfSet(self):
        def mock_glob(arg):
            if arg == '*.dar':
                return ['thing.0001.dar', 'thing.0002.dar', 'thing.0003.dar',
                        'thing.0004.dar']
            elif arg == '*.vol*.par2':
                return ['thing.0001-0004.vol000+500.par2']
            elif arg == '__disc*':
                return ['__disc1', '__disc2', '__disc3', '__disc4', '__disc5']
            else:
                return []
        self.d.wait_for_empty_disc = lambda: None
        with patch('shutil.move') as move, \
                patch('shutil.copyfile') as copyfile, \
                patch('os.unlink') as unlink, \
                patch('glob.glob', side_effect=mock_glob) as glob, \
                patch('io.open', new=FunTempFile) as open:
            self.d._create('dir', 'thing', '4', 'dar', 'operating')
            self.d._run.assert_any_call(
                    'par2', 'c', '-n1', '-u', '-r25',
                    'thing.0001-0004.par2',
                    'thing.0001.dar', 'thing.0002.dar',
                    'thing.0003.dar', 'thing.0004.dar')
            self.assertTrue(move.called)
            self.assertTrue(copyfile.called)
            self.d._run.assert_has_calls([
                call('growisofs', '-Z', '/dev/zero', '-R', '-J', '__disc1'),
                call('growisofs', '-Z', '/dev/zero', '-R', '-J', '__disc2'),
                call('growisofs', '-Z', '/dev/zero', '-R', '-J', '__disc3'),
                call('growisofs', '-Z', '/dev/zero', '-R', '-J', '__disc4'),
                call('growisofs', '-Z', '/dev/zero', '-R', '-J', '__disc5')])
            

class TestDarbrrbThreePlusEight(unittest.TestCase):
    def setUp(self):
        self.settings = Settings()
        self.settings.data_discs = 3
        self.settings.parity_discs = 8
        self.settings.burner_device = '/dev/zero'
        self.d = Darbrrb(self.settings, 'fnord')
        self.d._run = Mock()
        super().setUp()

    def testInvokeDar(self):
        self.d.settings.scratch_dir = sentinel.scratch_dir
        # we can't use a usual mock for this, because mocks have a name
        # attribute that means something else besides what a
        # NamedTemporaryFile's name attribute means, and we can't mock that
        # attribute.
        class FunTempFile:
            name = 'zobefuosfewboeuf'
            def __init__(self, *args, **kwargs):
                pass
            def __enter__(self, *args, **kwargs):
                return self
            def __exit__(self, *args, **kwargs):
                pass
            def write(self, *args, **kwargs):
                pass
            def flush(self, *args, **kwargs):
                pass
        with patch('os.chdir') as chdir, \
                patch('os.getcwd', return_value=sentinel.oldcwd) as getcwd, \
                patch('tempfile.NamedTemporaryFile', new=FunTempFile) as ntf:
            self.d.dar('-c', 'basename', '-R', '/home/bla/photos')
            chdir.assert_has_calls([call(sentinel.scratch_dir),
                    call(sentinel.oldcwd)])
            self.d._run.assert_called_with(
                    'dar', '-c', 'basename', '-R', '/home/bla/photos',
                    '-B', FunTempFile.name)

    def testFirstDiscOfSet(self):
        def mock_glob(arg):
            if arg == '*.dar':
                return ['thing.0001.dar']
            else:
                return []
        with patch('shutil.move') as move, \
                patch('shutil.copyfile') as copyfile, \
                patch('os.unlink') as unlink, \
                patch('glob.glob', side_effect=mock_glob) as glob:
            self.d._create('dir', 'thing', '1', 'dar', 'operating')
            self.assertFalse(move.called)
            self.assertFalse(copyfile.called)
            self.assertFalse(unlink.called)
            self.assertFalse(self.d._run.called)

    def testLastDiscOfSet(self):
        def mock_glob(arg):
            if arg == '*.dar':
                return ['thing.0001.dar', 'thing.0002.dar', 'thing.0003.dar']
            elif arg == '*.vol*.par2':
                return ['thing.0001-0003.vol000+100.par2',
                        'thing.0001-0003.vol000+200.par2',
                        'thing.0001-0003.vol000+300.par2',
                        'thing.0001-0003.vol000+400.par2',
                        'thing.0001-0003.vol000+500.par2',
                        'thing.0001-0003.vol000+600.par2',
                        'thing.0001-0003.vol000+700.par2',
                        'thing.0001-0003.vol000+800.par2']
            elif arg == '__disc*':
                return ['__disc1', '__disc2', '__disc3', '__disc4', '__disc5',
                        '__disc6', '__disc7', '__disc8', '__disc9', '__disc10',
                        '__disc11']
            else:
                return []
        self.d.wait_for_empty_disc = lambda: None
        with patch('shutil.move') as move, \
                patch('shutil.copyfile') as copyfile, \
                patch('os.unlink') as unlink, \
                patch('glob.glob', side_effect=mock_glob) as glob, \
                patch('io.open', new=FunTempFile) as open:
            self.d._create('dir', 'thing', '3', 'dar', 'operating')
            self.d._run.assert_any_call(
                    'par2', 'c', '-n8', '-u', '-r266',
                    'thing.0001-0003.par2',
                    'thing.0001.dar', 'thing.0002.dar',
                    'thing.0003.dar')
            self.assertTrue(move.called)
            self.assertTrue(copyfile.called)
            for disc in range(1, 8+1):
                self.d._run.assert_any_call(
                    'growisofs', '-Z', '/dev/zero',
                    '-R', '-J', '__disc{}'.format(disc))



if __name__ == '__main__':
    s = Settings()
    if len(sys.argv) < 2:
        usage(s)
        sys.exit(1)
    opts, remaining = getopt.getopt(sys.argv[1:], 'hvnt', ['help'])
    for o, v in opts:
        if o == '-h' or o == '--help':
            usage(s)
            sys.exit(1)
        elif o == '-v':
            s.verbose = True
        elif o == '-n':
            s.dry_run = True
        elif o == '-t':
            sys.argv = [sys.argv[0]] + remaining
            unittest.main()
        else:
            raise Exception('unknown switch {}'.format(o))
    d = Darbrrb(s, sys.argv[0])
    if remaining[0] == 'dar':
        d.ensure_scratch()
        d.dar(remaining[1:])
    elif remaining[0] == '_create':
        d.create(remaining[1:])


