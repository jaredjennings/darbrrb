darbrrb
=======

This is dar-based Blu-Ray redundant backup. It exists to back up a few hundred
gigabytes of data onto dozens of optical discs in a way that it can be restored
about six years later.

Setting
-------

I've got a few hundred gigabytes of data and a fireproof box. My data is on an
LVM logical volume, so I can make a snapshot to ensure it won't change during
backup. This is my data I'm backing up, not my system, so I don't need
bare-metal restore.

Problems
--------

When I tried to restore my DVD backup from six years ago, made with dar and par2:

1. I couldn't figure out how I ran dar to make the backup, nor how to run it to
   restore the files.
2. Half the DVDs had scratches, and on those discs I/O errors caused the
   copying of the single, giant 4.5GB file to stop halfway through.
3. I had par2 files that gave me 5% data redundancy. These were not enough, and
   they were on the same disc as the data that was lost.
4. Nothing could save me if I had lost a whole disc.

Solutions
---------

We use [http://dar.linux.free.fr](dar) and
[http://en.wikipedia.org/wiki/Parchive](par)2 because they work well and are
stable over a period of years. We guard against localized disc errors by using
dar slice files smaller than the disc. We guard against the loss of an entire
disc by using heavy redundancy with the aid of par2. We guard against forgetful
users by putting complete documentation of how the backup was made on every
disc.
