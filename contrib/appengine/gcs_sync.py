import os
import sys
from base64 import b64encode
from hashlib import md5
import inotify.adapters
import signal
from time import time, sleep
from queue import Queue, Empty
from threading import Thread, Event
from google.cloud import storage
from google.cloud.exceptions import NotFound
import magic
from http.server import HTTPServer, BaseHTTPRequestHandler

SYNC_DELAY = 30
MONITOR_PERIOD = 30

def read_file(path):
    u"""Read a file as bytes"""
    try:
        with open(path, mode='rb') as file:
            return file.read()
    except:
        return None

def get_all_nowait(queue):
    u"""Generate all of the items in the queue, until it blocks"""
    while True:
        try:
            yield queue.get(False)
            queue.task_done()
        except Empty:
            return

def md5_hash(contents):
    return b64encode(md5(contents).digest()).decode('utf-8')


class GCSStorage:
    u"""Class to upload and download files from Google Cloud Storage."""
    def __init__(self, data_dir, bucket_name):
        self.data_dir = data_dir
        self.bucket_name = bucket_name
        self.hashes = {}

    def _backup_name(self, name, hash):
        return os.path.join("~backup", name+"~"+hash)

    def _is_ignored(self, name):
        return name.startswith("~backup/")

    def upload_file(self, name, contents):
        u"""Upload or delete a file.

        Before uploading, this will check that the target blob has not been
        unexpectedly edited.

        Args:
            name (str): path to the file relative to `self.data_dir` (and name of blob)
            contents (bytes): data to write to the bucket.
                If contents is None, the file will be deleted from the bucket.
        """
        if self._is_ignored(name):
            print("Ignoring {}".format(name))
            return
        print("uploading file {} ({})".format(name, contents is None))
        gcs = storage.Client()
        bucket = gcs.get_bucket(self.bucket_name)
        blob = bucket.blob(name)
        try:
            blob.reload()
        except NotFound:
            pass

        if name in self.hashes and self.hashes[name] != blob.md5_hash:
            backup_name = self._backup_name(name, blob.md5_hash)
            print("Blob {} was changed unexpectedly ({} != {}). Backing up remote file to {}.".format(blob.name, blob.md5_hash, self.hashes[name], backup_name))
            contents = blob.download_as_string()
            bucket.blob(backup_name).upload_from_string(contents, content_type=blob.content_type)

        path = os.path.join(self.data_dir, name)
        if contents is None:
            if name in self.hashes:
                print("Deleting file {}".format(name))
                blob.delete()
                del self.hashes[name]
            return
        local_hash = md5_hash(contents)
        if local_hash == self.hashes.get(name):
            print("Local file {} matches remote file".format(name))
            return
        print("Local file {} changed. Uploading...".format(name))
        content_type = blob.content_type
        if content_type is None:
            mime = magic.Magic(mime=True)
            content_type = mime.from_file(path)
        blob.upload_from_string(contents, content_type=content_type)
        print("Local file {} uploaded.".format(name))
        self.hashes[name] = local_hash

    def download_all_files(self):
        u"""Dowload all files from the bucket.

        Files that already exist locally are not overwritten; an exception is
        raised if a local file exists that doesn't match the bucket.

        Args:
            dir (str): path to the directory to save the files
        """
        print("Downloading files from bucket {} into directory {}.".format(self.bucket_name, self.data_dir))
        gcs = storage.Client()
        bucket = gcs.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            print("blob: {}".format(blob.name))
            if self._is_ignored(blob.name):
                print("Ignoring {}.".format(blob.name))
                continue
            path = os.path.join(self.data_dir, blob.name)
            self.hashes[blob.name] = blob.md5_hash
            if path.endswith("/"):
                os.makedirs(path, exist_ok=True)
                continue
            if os.path.exists(path):
                if not os.path.isfile(path):
                    raise Exception("Blob path {} corresponds to a local directory".format(blob.name))
                local_contents = read_file(path)
                local_hash = md5_hash(local_contents)
                if local_hash == blob.md5_hash:
                    continue
                else:
                    backup_name = self._backup_name(blob.name, local_hash)
                    print("Blob {} exists locally, but the hash does not match ({} != {}). Backing up local file to {}.".format(
                        blob.name, local_hash, blob.md5_hash, backup_name))
                    bucket.blob(backup_name).upload_from_string(local_contents, content_type=blob.content_type)

            os.makedirs(os.path.dirname(path), exist_ok=True)
            blob.download_to_filename(path)

        print("Done downloading.")
        for root, dirs, files in os.walk(self.data_dir):
            for name in files:
                path = os.path.join(root, name)
                name = os.path.relpath(path, self.data_dir)
                if name not in self.hashes and not self._is_ignored(name):
                    local_contents = read_file(os.path.join(self.data_dir, name))
                    local_hash = md5_hash(local_contents)
                    backup_name = self._backup_name(name, local_hash)
                    print("Local directory contains file {}, which is not in the bucket. Backing up local file to {}, and removing.".format(name, backup_name))
                    bucket.blob(backup_name).upload_from_string(local_contents, content_type=blob.content_type)
                    os.remove(path)
        print("Done checking for extra files")


class State:
    def __init__(self):
        self._terminating = False
        self._watcher_terminated = False
        self._watcher_heartbeat = 0
        self._upload_terminated = False
        self._upload_heartbeat = 0

    def is_healthy(self):
        now = time()

        healthy = not self._terminating

        healthy = healthy and (now - self._watcher_heartbeat < 2)
        healthy = healthy and not self._watcher_terminated

        healthy = healthy and (now - self._upload_heartbeat < 2)
        healthy = healthy and not self._upload_terminated

        return healthy

    def terminate(self):
        self._terminating = True

    def is_terminating(self):
        return self._terminating

    def watcher_heartbeat(self):
        self._watcher_heartbeat = time()

    def watcher_terminated(self):
        self._watcher_terminated = True

    def upload_heartbeat(self):
        self._upload_heartbeat = time()

    def upload_terminated(self):
        self._upload_terminated = True

    def on_signal(self, num, frame):
        if num == signal.SIGINT or num == signal.SIGTERM:
            self._terminating = True


def upload(storage, queue, state):
    u"""Upload files whenever there is a changed.

    Args:
        bucket_name (Storage): The backing storage to upload changed files.
        queue (Queue): The queue to get events from.
            Each event must be a pair `(path, time)`, where `path` is the path,
            relative to `dir`, and `time` is the time when the event was put
            into the queue.
        state (State): Object used to signal health, and to query for shutdown.
    """
    print("Starting upload process...")
    try:
        while not state.is_terminating():
            state.upload_heartbeat()

            # block until the queue has some event
            try:
                name, event_time = queue.get(timeout=1)
                queue.task_done()
            except Empty:
                continue

            # initialize the set of modified paths to be just the one from the event
            paths = {}
            paths[name] = read_file(os.path.join(storage.data_dir, name))

            now = time()

            # loop as long as the latest event occured less than SYNC_DELAY seconds ago
            while now - event_time < SYNC_DELAY:
                state.upload_heartbeat()

                # sleep for approximately long enough for the latest event to be at least SYNC_DELAY seconds old
                sleep(SYNC_DELAY - (now - event_time) + 0.01)

                # Pull all the events out of the queue, putting the paths into the set of paths, and updating the time of the latest event
                for event in get_all_nowait(queue):
                    name, event_time = event
                    paths[name] = read_file(os.path.join(storage.data_dir, name))

                now = time()

            # Now there has been a delay of at least SYNC_DELAY seconds since the last modification, time to upload
            for name, contents in paths.items():
                storage.upload_file(name, contents)
    finally:
        state.upload_terminated()

    # Now shutting down now
    print("Shutting down upload thread...")

    # wait for any last events from the watcher
    sleep(0.1)

    paths = {}
    for event in get_all_nowait(queue):
        name, _ = event
        paths[name] = read_file(os.path.join(storage.data_dir, name))
    for name, contents in paths.items():
        storage.upload_file(name, contents)
    print("Upload thread terminated.")


def watch(dir, queue, state):
    u"""Watch a directory for any files that change.

    Args:
        dir (string): The directory to watch.
        queue (Queue): The queue to put file change events.
            Each event is a pair `(path, time)`, where `path` is the path,
            relative to `dir`, and `time` is the time when the event was put
            into the queue.
        state (State): Object used to signal health, and to query for shutdown.
    """
    print("Starting watcher thread...")
    try:
        watcher = inotify.adapters.InotifyTree(dir)
        while not state.is_terminating():
            state.watcher_heartbeat()
            for event in watcher.event_gen(yield_nones=False, timeout_s=1):
                state.watcher_heartbeat()
                (_, type_names, path, filename) = event

                write = False
                for t in type_names:
                    if t == 'IN_CLOSE_WRITE' or t == 'IN_DELETE':
                        write = True

                if write:
                    print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}".format(
                        path, filename, type_names))
                    full_path = os.path.join(path, filename)
                    rel_path = os.path.relpath(full_path, dir)
                    queue.put((rel_path, time()))
    finally:
        state.watcher_terminated()
    print("Watcher thread terminated.")


def monitor(state):
    print("Monitor thread started.")
    while state.is_healthy():
        # sleep for approximately long enough for the latest event to be at least SYNC_DELAY seconds old
        sleep(MONITOR_PERIOD)
    print("Monitor thread terminated.")
    state.terminate()
    raise SystemExit


def gcs_sync(watch_dir, bucket_name):
    queue = Queue()
    print("Constructiong GCSStorage...")
    storage = GCSStorage(watch_dir, bucket_name)
    print("Done constructing GCSStoorage.")
    storage.download_all_files()

    state = State()
    try:
        signal.signal(signal.SIGINT, state.on_signal)
        signal.signal(signal.SIGTERM, state.on_signal)
    except ValueError:
        pass

    watch_thread = Thread(target=watch, args=(watch_dir, queue, state), daemon=False)
    watch_thread.start()

    upload_thread = Thread(target=upload, args=(storage, queue, state), daemon=False)
    upload_thread.start()

    monitor_thread = Thread(target=monitor, args=(state,), daemon=False)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("USAGE: {} <watch_dir> <bucket_name>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(-1)
    watch_dir = sys.argv[1]
    bucket_name = sys.argv[2]
    gcs_sync(watch_dir, bucket_name)
