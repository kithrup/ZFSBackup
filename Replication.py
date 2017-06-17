from __future__ import print_function
import os, sys
import subprocess

class ZFSReplicationError(ValueError):
    pass

class ZFSReplication(object):
    def __init__(self, src_ds, dst_ds,
                 recursive=False):
        self._source = src_ds
        self._destination = dst_ds
        self._recursive = recursive
        print(self, file=sys.stderr)
        
    def __repr__(self):
        return "ZFSReplication('{}', '{}', recursive={})".format(
            self.source, self.destination, self.recursive)
    
    def Snapshots(self):
        # Return an array of snapshots for the destination.
        # sorted by creation time.
        # Return value is an array of dictionaries.
        # This would be easier with libzfs.
        command = ["/sbin/zfs", "list", "-H", "-p", "-o", "name,creation",
                                     "-s", "creation", "-r"]
        if not self.recursive:
            command.extend(["-d", "1"])
        command.append(self.destination)
        try:
            output = subprocess.check_output(command)
            print("output = {}".format(output), file=sys.stderr)
        except subprocess.CalledProcessError:
            raise ZFSReplicationError("Cannot get snapshots on destination {}".format(self.source))
        snapshots = []
        print("output = {}".format(output), file=sys.stderr)
        for snapshot in output:
            if not snapshot:
                continue
            t = snapshot.rstrip().split()
            print("t = {}".format(t), file=sys.stderr)
            snapshots.append({ "Name" : t[0], "CreationTime" : t[1] })
        return snapshots

    def validate(self):
        # Make sure the parameters are okay.
        # For this base class, this means making
        # sure that source and dest don't overlap
        # (if recursive), or aren't the same (if not
        # recursive).
        if not self.source:
            raise ZFSReplicationError("Source dataset must be set")
        if not self.destination:
            raise ZFSReplicationError("Destination dataset must be set")
        if self.recursive:
            src_path_comps = self.source.split("/")
            dst_path_comps = self.destination.split("/")
            if src_path_comps == dst_path_comps[:len(src_path_comps)]:
                raise ZFSReplicationError("Recursive replication cannot overlap")
        else:
            if self.source == self.destination:
                raise ZFSReplicationError("Source and destination datasets cannot be the same")
        # Ensure destination does not overlap itself
        # What that means is, tank/foo -> tank would be an error.
        # Essentially, see if dirname(src) == dst
        if os.path.dirname(self.source) == self.destination:
            raise ZFSReplicationError("Replication would over-write itself")
        # The destination cannot be the root dataset
        if len(self.destination.split("/")) == 1:
            raise ZFSReplicationError("Replication cannot be to pool")

        return

    @property
    def source(self):
        return self._source
    @property
    def destination(self):
        return self._destination
    @property
    def recursive(self):
        return self._recursive
    
try:
    repl = ZFSReplication("zroot", "zroot/backup", recursive=True)
    repl.validate()
except ZFSReplicationError:
    print("Got expected error for zroot -> zroot/backup recursive")
    pass
else:
    raise Exception("Expected an error but did not get one!")

# This one should not get an error
repl = ZFSReplication("zroot", "zroot/backup")
repl.validate()

# This should not get an error
repl = ZFSReplication("zroot/TestDataset", "zroot/var/tmp")
repl.validate()
print("Snapshots on source = {}".format(repl.SourceSnapshots()))

# This one should not get an error
repl = ZFSReplication("zroot/backup/Media", "zroot", recursive=True)
repl.validate()

# This one should get an error
try:
    repl = ZFSReplication("zroot/Data/Media", "zroot/Data")
    repl.validate()
except ZFSReplicationError:
    print("Got expected error zroot/Data/Media -> zroot/Data")
else:
    raise Exception("Expected an error but did not get one!")

# Should work
repl = ZFSReplication("zroot", "tank/Backup", recursive=True)
