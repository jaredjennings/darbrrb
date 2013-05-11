#!/bin/bash
# See usage function below for documentation. 
# vvvvvvvv    Below are variables for you to mess with    vvvvvvvvvvvvv

DIGITS=4
VERBOSE=0
DRY_RUN=0

# figures in mebibytes
DISC_SIZE=650
SLICE_SIZE=64

# each redundancy set is composed of (SET_SIZE + PARITY) discs
# these are like hard disk shelves with RAID, but with discs instead
SET_SIZE=4 #discs
PARITY=1   #discs

# SCRATCH_DIR must have (SET_SIZE + PARITY) * DISC_SIZE mebibytes free to run
# backup; see ensure_free_space below. SCRATCH_DIR must not contain anything at
# the beginning. SCRATCH_DIR must not be a subdirectory of the directory being
# backed up.
SCRATCH_DIR=scratch

# ^^^^^^^^    Above are variables for you to mess with    ^^^^^^^^^^^

SCRATCH_FREE_NEEDED=$(( ( $SET_SIZE + $PARITY ) * $DISC_SIZE )) # MiB

usage () {
    cat >&2 <<EOF

This script wraps dar and par2 in such a way to make compressed, encrypted
backups with ${SLICE_SIZE} MiB slices striped across sets of ${DISC_SIZE} MiB optical discs,
each set containing ${SET_SIZE} data disc(s) and ${PARITY} parity disc(s).

When backing up, the directory "${SCRATCH_DIR}" should have $SCRATCH_FREE_NEEDED MiB of space free.
When restoring, copy this script off of the optical disc first; you'll need to
switch optical discs during the backup.

Usage: bash $0 [-v] [-n] dar <dar parameters>

Dar parameters of note:
    -c <archive basename> -R <dir containing files to backup> 
    -x <archive basename>

See dar(1) about parameters you can give to dar. This script provides a darrc
that gives a bunch of detailed switches, so you probably only need to hit the
high points: telling dar which operation to perform, and specifying which files
it's dealing with.

The -v switch, before dar, means to be verbose and show the dar command being
executed and the darrc used. The -n switch, before dar, means dry run or
no-act: don't actually execute anything.

EOF
}


REDUNDANCY_PERCENT=$(( $PARITY * 100 / ( $SET_SIZE + $PARITY ) ))

run () {
    if [ $VERBOSE = 1 ]; then echo "$@"; fi
    if [ $DRY_RUN = 0 ]; then "$@"; fi
}

dar_ () {
    DARRC=$(mktemp)
    cat > $DARRC <<EODARRC

--min-digits=$DIGITS
--compression=bzip2
--slice ${SLICE_SIZE}M
# make crypto block size larger to reduce likelihood of duplicate ciphertext
--crypto-block 131072
--key aes:
-v

create:
        -E bash "$0" _create %p %b %N %e %c
test:
        -E bash "$0" _test %p %b %N %e %c

EODARRC
    if [ $VERBOSE = 1 ]; then
        cat <<EOV

vvvvvvvv Contents of darrc file $DARRC:     vvvvvvvvvvv
$(cat $DARRC)
^^^^^^^^ End contents of darrc file $DARRC. ^^^^^^^^^^^

EOV
    fi
    run dar -B $DARRC "$@"
    rm -f $DARRC
}

ensure_free_space () {
    blocks_free=$(stat -f -c %f $SCRATCH_DIR)
    block_size=$(stat -f -c %S $SCRATCH_DIR)
    mb_free=$(( $blocks_free * $block_size / 1048576 ))
    if [ $mb_free -lt $SCRATCH_FREE_NEEDED ]; then
        echo "Not enough free space on $SCRATCH_DIR directory" >&2
        echo "Need $mb_needed MB; only have $mb_free" >&2
        exit 8
    fi
}

ensure_scratch () {
    if [ -e $SCRATCH_DIR ]; then
        if ! rmdir $SCRATCH_DIR >&/dev/null; then
            echo "$SCRATCH_DIR already exists; aborting" >&2
            exit 7
        fi
    fi
    mkdir -p $SCRATCH_DIR
    ensure_free_space
}

_create () {
    echo "_create $@"
}

_test () {
    echo "_test $@"
}

deal_with_args () {
    echo "dealing with args $*"
    case "$1" in
        -h|-help|--help)
            usage
            exit 1
            ;;
        -v)
            VERBOSE=1
            shift
            deal_with_args "$@"
            ;;
        -n)
            DRY_RUN=1
            shift
            deal_with_args "$@"
            ;;
        dar)
            ensure_scratch
            shift
            dar_ "$@"
            ;;
        _create)
            shift
            _create "$@"
            ;;
        _test)
            shift
            _test "$@"
            ;;
    esac
}

# ---------------------------------------------------------------------------
#                              end of functions
#                           the main script follows
# ---------------------------------------------------------------------------

if [ $# = 0 ]; then
    usage
    exit 1
fi
deal_with_args "$@"
