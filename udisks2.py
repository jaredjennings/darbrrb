import sys
import logging
import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject
from multiprocessing import Process, Queue
from contextlib import contextmanager

OFUD2Path = '/org/freedesktop/UDisks2'

class OFUD2:
    TOP = 'org.freedesktop.UDisks2'
    Block = TOP + '.Block'
    Filesystem = TOP + '.Filesystem'
    Drive = TOP + '.Drive'

OFDOM = 'org.freedesktop.DBus.ObjectManager'
OFUDENACO = 'org.freedesktop.UDisks2.Error.NotAuthorizedCanObtain'

def eject(drive_bus_name):
    bus = dbus.SystemBus()
    obj = bus.get_object(OFUD2.TOP, drive_bus_name)
    di = dbus.Interface(obj, OFUD2.Drive)
    di.Eject({})

def process(q):
    FORMAT = '%(asctime)-15s %(levelname)s %(name)s %(message)s'
    logging.basicConfig(format=FORMAT, stream=sys.stderr, level=logging.DEBUG)
    top = logging.getLogger(__name__)

    top.debug('beginning')
    DBusGMainLoop(set_as_default=True)

    # http://stackoverflow.com/questions/5067005/python-udisks-enumerating-device-information
    bus = dbus.SystemBus()
    ud_om_obj = bus.get_object(OFUD2.TOP, '/org/freedesktop/UDisks2')
    ud_om = dbus.Interface(ud_om_obj, OFDOM)

    def listen_for_empties(bus_name, device_file, media_types=()):
        log = logging.getLogger(__name__+'.listen_for_empties')
        if len(media_types) == 0:
            media_good = lambda x: True
        else:
            media_good = lambda x: x in media_types
        def changed(interface_name, changed, invalidated):
            if (
                    changed.get('MediaAvailable', False) and
                    changed.get('Optical', False) and
                    changed.get('OpticalBlank', False)):
                media_type = changed.get('Media', '')
                log.info('A medium of type {} was '
                              'inserted.'.format(media_type))
                if media_good(media_type):
                    # proper kind of empty disc inserted
                    q.put(('blank', str(device_file), str(bus_name)))
                else:
                    log.info('The medium was not of the desired type.')
                for k, v in changed.items():
                    log.debug('{} is now {!r}'.format(k, v))
        bus.add_signal_receiver(changed, 'PropertiesChanged',
                                dbus.PROPERTIES_IFACE, path=bus_name)

    def listen_and_mount_data_discs(block_bus_name, drive_bus_name):
        log = logging.getLogger(__name__+'.listen_and_mount_data_discs')
        def added(object_path, interfaces_and_properties):
            log.debug('InterfacesAdded event')
            log.debug(object_path)
            log.debug(repr(interfaces_and_properties))
            if object_path == block_bus_name:
                for ifname, properties in interfaces_and_properties.items():
                    if ifname == OFUD2.Filesystem:
                        obj = bus.get_object(OFUD2.TOP, block_bus_name)
                        fsi = dbus.Interface(obj, OFUD2.Filesystem)
                        log.info('fs detected on {}, mounting'.format(block_bus_name))
                        try:
                            mtpt = fsi.Mount({'auth.no_user_interaction': True})
                            log.info('mounted at {}'.format(mtpt))
                            q.put(('filesystem', str(mtpt),
                                   str(drive_bus_name)))
                        except dbus.exceptions.DBusException as e:
                            self.log.error('mount failed: {}: {}'.format(
                                e.get_dbus_name(), e.get_dbus_message()))
        bus.add_signal_receiver(added, 'InterfacesAdded', OFDOM,
                                path=OFUD2Path)

    top.debug('enumerating devices')
    erthing = ud_om.GetManagedObjects()
    for name, info in erthing.items():
        for interface_name, getall in info.items():
            if interface_name == OFUD2.Block:
                device_file_name = (bytes(getall['Device']).
                                    rstrip(b'\x00').decode('ascii'))
                drive_path = getall['Drive']
                if drive_path != '/': # if it is, this Block has no Drive
                    drive_getall = erthing[drive_path][OFUD2.Drive]
                    if (
                            drive_getall['MediaRemovable'] and
                            drive_getall['MediaChangeDetected'] and 
                            any(z.startswith('optical')
                                for z in drive_getall['MediaCompatibility'])):
                        top.info('drive with removable '
                                 'optical media {}'.format(device_file_name))
                        listen_for_empties(drive_path, device_file_name)
                        listen_and_mount_data_discs(name, drive_path)
    loop = GObject.MainLoop()
    loop.run()

@contextmanager
def disc_inserted_queue():
    q = Queue()
    p = Process(target=process, args=(q,))
    p.start()
    yield q
    q.close()
    p.terminate()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    with disc_inserted_queue() as q:
        while True:
            print(q.get())
