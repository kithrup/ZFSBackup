from __future__ import print_function
import os, sys
import subprocess
import time

class ZFSReplicationError(ValueError):
    pass

class ZFSReplication(object):
    """
    Base class for doing ZFS replication.
    """
    def __init__(self, target):
        # The only thing we need to do here is ensure the target
        # exists.  We'll call a method to validate it, so subclassing
        # works
        self._dest = target
        self.validate()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.target)
    
    def validate(self):
        """
        Ensure the destination exists.  Derived classes will want
        to override this.
        This would be better with libzfs.
        """
        command = ["/sbin/zfs", "list", "-H", self.target]
        try:
            with open("/dev/null", "w") as devnull:
                subprocess.check_call(command, stdout=devnull, stderr=devnull)
        except subprocess.CalledProcessError:
            raise ZFSReplicationError("Target {} does not exist".format(self.target))
        return

    def replicate(self, source, name, date=None, recursive=False):
        """
        Replicate from source.  source must be an object that supports
        read().  If date is not given, we will use the current time, so
        it should really be set.
        The name, date, and recursive parameters are for informational
        purposes in the base class, but may be used for other purposes
        in derived classes
        """
        command = ["/sbin/zfs", "receive", "-d", "-F", self.target]
        try:
            subprocess.check_call(command, stdin=source)
        except subprocess.CalledProcessError:
            raise ZFSReplicationerror("Could not replicate {} to target {}".format(name, self.target))
        return
    
    @property
    def target(self):
        return self._dest

    @property
    def snapshots(self):
        """
        Return an array of snapshots for the destination.
        Each entry in the array is a dictonary with at least
        two keys -- Name and CreationTime.  CreationTime is
        an integer (unix seconds).  The array is sorted by
        creation time (oldest first).  If there are no snapshots,
        an empty array is returned.
        This would be better with libzfs.
        """
        command = ["/sbin/zfs", "list", "-H", "-p", "-o", "name,creation",
                   "-r", "-d", "1", "-t", "snapshot", "-s", "creation",
                   self.target]
        try:
            output = subprocess.check_output(command).split("\n")
        except subprocess.CalledProcessError:
            # We'll assume this is because there are no snapshots
            return []
        snapshots = []
        for snapshot in output:
            if not snapshot:
                continue
            (name, ctime) = snapshot.rstrip().split()
            snapshots.append({"Name" : name, "CreationTime" : int(ctime) })
        return snapshots

class ZFSReplicationCount(ZFSReplication):
    def __init__(self, target):
        super(ZFSReplicationCount, self).__init__(target)
        self._count = 0
        
    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.target)

    def validate(self):
        return

    def replicate(self, source, name, date=None, recursive=False):
        """
        Just count the characters
        """
        count = 0
        mByte = 1024 * 1024
        while True:
            buf = source.read(mByte)
            if buf:
                count += len(buf)
            else:
                break
            
        self._count = count
        
    @property
    def snapshots(self):
        return []
    @property
    def count(self):
        return self._count
    
if __name__ == "__main__":
    for ds in sys.argv[1:]:
        target = ZFSReplication(ds)
#        print("Target = {}".format(target))
#        print(target.snapshots)
        target = ZFSReplicationCount(ds)
        print("Target = {}".format(target))
        # Should not hardcode this, even for testing.
        snapname = "zroot/usr/home@auto-snap-mgmt-2017-06-16_20.50"
        command = ["/sbin/zfs", "send", snapname]
        with open("/dev/null", "rw") as devnull:
            mByte = 1024 * 1024
            snap_io = subprocess.Popen(command,
                                       bufsize=mByte,
                                       stdin=devnull,
                                       stdout=subprocess.PIPE,
                                       stderr=devnull)
            target.replicate(snap_io.stdout, snapname)
            print("{} bytes in snapshot".format(target.count))
