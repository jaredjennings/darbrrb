# Functions to calculate the expected size of a par2 file, given a few things
# about it.

def pad_to_4(n):
        padding = 4 - (n % 4)
        if padding == 4: padding = 0
        return n + padding

def packet_size(data_size):
	return 8 + 8 + 16 + 16 + 16 + pad_to_4(data_size)

def main_packet_size(n_recovery_files, n_non_recovery_files=0):
	body_size = 8 + 4 + n_recovery_files * 16 + n_non_recovery_files * 16
	return packet_size(body_size)

def file_description_packet_size(filename):
        body_size = 16 + 16 + 16 + 8 + pad_to_4(len(filename))
        return packet_size(body_size)

def input_file_slice_checksum_packet_size(n_slices):
        body_size = 16 + n_slices * 20
        return packet_size(body_size)

def recovery_slice_packet_size(n_bytes):
        body_size = 4 + pad_to_4(n_bytes)
        return packet_size(body_size)

def creator_packet_size(creator='Created by par2cmdline version 0.4.'):
        return packet_size(pad_to_4(len(creator)))

# assumption: files are all of the same size.
# assumption: we will have redundancy rate of n_recovery_files / len(filenames)
def par_recovery_size(filenames, filesize, n_recovery_files):
        # FIXME: how do we know it will be 660 slices?
        slices_per_file = 660
        slice_size = filesize // slices_per_file
        size = 0
        # FIXME: how do we know it will be 10 main packets?
        size += 10 * main_packet_size(len(filenames))
        for fn in filenames:
                size += file_description_packet_size(fn)
        size += len(filenames) * \
                input_file_slice_checksum_packet_size(slices_per_file)
        for n in range(slices_per_file):
                size += recovery_slice_packet_size(slice_size)
        return size

if __name__ == '__main__':
        print("expecto size {}".format(par_recovery_size(
               ['foo.0001.dar', 'foo.0002.dar', 'foo.0003.dar'],
               5242880, 2)))
