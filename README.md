## Synopsis

ZFSBackup is a Python module to backup ZFS pools and datasets;
it's written in an object-oriented fashion to allow for different
backup methods.

Note that ZFSBackup backs up pools and datasets, _not_ individual
files.

## API Reference

The base class is ZFSBackup:

    backup = ZFSBackup("tank", "BackupPool/tank", recursive=True)

This creates an object which can be used to backup "tank" to "BackupPool/tank",
recursively.  The other classes are

* ZFSBackupSSH
* ZFSBackupDirectory
* ZFSBackupS3
* ZFSBackupCount

Backups are initiated by

    backup.backup(snapname=None, force_full=False)

Specifying a snapshot name will use that; otherwise it will use
the most recent snapshot.  Unless told not to, it will attempt
to do an incremental backup, by quering the targt for a list of
snapshots (how depends on which class is used).

### Filters

Backups can have filters applied, using the class ZFSBackupFilter.
The classes are

* ZFSBackupFilterThread
* ZFSBackupFilterCommand
* ZFSBackupFilterCompressed
* ZFSBackupFilterCounter
* ZFSBackupFilterEncrypted

An example use:

    backup.AddFilter(ZFSBackupFilterCompressed())

which will interpose gzip between the source and target.  (Some
classes may ignore the filters, as they don't make sense.)  Using
ZFSBackupFilterCommand, any command can be used; the constructor
should be given an array of command and arguments -- one for
backing up, and one for restoring.  As an example:

    filter = ZFSBackupFilterCommand(backup_command=["/bin/cat"])

## Command-line Usage

In general, you need root access to run zfs send.

The layout for the command-line is

    ZFSBackup [common-options] method [method-options]
    
### Common command-line options

* --dataset/--pool or --snapshot

  This specifies which dataset/pool.  One of these options is required.  Currently they are all treated the same.
* --debug
* --verbose
* --operation backup|list

  This specifies the operation to perform; the default is backup.
* --compressed, -C

  Compress the backup.  The default is to use gzip, but with --pigz, it will use /usr/local/bin/pigz to compress.  This is performed using the ZFSBackupFilterCompressed filter.
* --encrypted, -E

  Encrypt the backup.  The default is no encryption.  When using encryption, --password-file=/path *must*
be specified.  Optionally, a cipher may be specified with --cipher=cipher; the default is to use the
aes-256-cbc cipher.  Valid ciphers are determined by using 'openssl list-cipher-commands'.
* --recursive, -R

  Recursively perform the operation.  Currently only valid with backup.  The default is to not use recursion.

### ZFS replication options
The 'zfs' method replicates from one ZFS pool/dataset to another ZFS dataset.  There is only one, mandatory option:

* --dest, -D

  Specify the destination dataset.

### Directory backup options
The "directory" method saves snapshots as files in the given directory.  It breaks the snapshot down into
multiple chunks, with a maximum size of 2GBytes.  It maintains a map file, which is a JSON file with the
source dataset as the primary key, and then information about each snapshot for the source.

The options for the "directory" method are:

* --dest, -D

  The destination directory.  This is required.
* --prefix, -P

  A prefix to use.  By default, it will use the hostname.

Backups are thus stored in DEST/PREFIX/chunks/SNAPSHOTCHUNKFILE

### S3 backup options
The "s3" method is similar to the "directory" method, but it uses an Amazon S3-compatible
server (it has been tested with Amazon's S3, and with the Minio server).
It uses a largr chunk size (4GBytes), but otherwise behaves the same as the "directory"
method.  When glacier transition is enabled (the default), it will set up a transition
rule for the given bucket and prefix that will migrate the chunk objects to glacier storage
after 1 day.

The "s3" options are:

* --bucket

  The name of the bucket to use.  S3 bucket names must be globally unique on the server.
* --prefix

  A prefix to use in the bucket.  This defaults to the hostname.
* --key

  The access key used to access the service.  The key must be able to list, create, and
set lifecycle configuration on buckets, as well as list, create, and delete files within
the bucket.  The access key is required.
* --secret

  The secret associated with the given key.  This is required.
* --server

  The URL for an S3-compatible server.  The default is to use Amazon.
* --glacier, --no-glacier

  Whether or not to use glacier.  The default is to use glacier.
* --region

  The region to use.

### SSH Backup
The 'ssh' method replicates the pool/dataset to a remote ZFS dataset, using
ssh.  The options are:

* --dest, -D

  The target dataset to backup to.  This option is required.
* --host, -H

  The remote host to backup to.  This option is required.
* --user, -U

  The username to use.  Default is to use the current user.

It is strongly recommended that password-less login be set up using
public and private keys.

## Limitations

* Restore is not yet implemented.
* There is currently no way to monitor progress properly.
* Errors in the sub-processes and filters don't propagate.
