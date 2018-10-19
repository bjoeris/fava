"""Reading/writing Beancount files in Google Cloud Storage."""

import codecs
from hashlib import sha256
import os
import re

from beancount.core import data, flags
from beancount.parser.printer import format_entry

from fava.core.helpers import FavaAPIException, FavaModule
from fava.core.misc import align

from beancount.utils import misc_utils
from beancount.core import data
from beancount.parser import parser
from beancount.parser import booking
from beancount.parser import options
from beancount.parser import printer
from beancount.ops import validation
from beancount.utils import encryption
from beancount.utils import file_utils

from google.cloud import storage
from google.cloud.exceptions import NotFound

def _parse_recursive_gcs(sources, log_timings, extra_validations, encoding):
    """Parse Beancount input, run its transformations and validate it.

    Recursively parse a list of files or strings and their include files and
    return an aggregate of parsed directives, errors, and the top-level
    options-map. If the same file is being parsed twice, ignore it and issue an
    error.

    Args:
      sources: A list of (filename-or-string, is-filename) where the first
        element is a string, with either a filename or a string to be parsed directly,
        and the second arugment is a boolean that is true if the first is a filename.
        You may provide a list of such arguments to be parsed. Filenames must be absolute
        paths.
      log_timings: A function to write timings to, or None, if it should remain quiet.
      encoding: A string or None, the encoding to decode the input filename with.
    Returns:
      A tuple of (entries, parse_errors, options_map).
    """
    assert isinstance(sources, list) and all(isinstance(el, tuple) for el in sources)

    storage_client = storage.Client()

    # Current parse state.
    entries, parse_errors = [], []
    options_map = None

    # A stack of sources to be parsed.
    source_stack = list(sources)

    # A list of absolute filenames that have been parsed in the past, used to
    # detect and avoid duplicates (cycles).
    filenames_seen = set()

    with misc_utils.log_time('beancount.parser.parser', log_timings, indent=1):
        while source_stack:
            source, is_file = source_stack.pop(0)
            is_top_level = options_map is None

            if is_file:
                # All filenames here must be absolute.
                prefix = "gs://"
                assert source.startswith(prefix)
                bucket_name, file_name = source[len(prefix):].split("/", 1)

                # Check for file previously parsed... detect duplicates.
                if source in filenames_seen:
                    parse_errors.append(
                        LoadError(data.new_metadata("<load>", 0),
                                  'Duplicate filename parsed: "{}"'.format(source),
                                  None))
                    continue

                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob()
                try:
                    source = blob.download_as_string()
                    filenames_seen.add(source)

                    cwd = path.dirname(filename)
                except NotFound:
                    # File that does not exist.
                    parse_errors.append(
                        LoadError(data.new_metadata("<load>", 0),
                                  'File "{}" does not exist'.format(filename), None))
                    continue

            else:
                # If we're parsing a string, there is no CWD
                cwd = None

            # Encode the contents if necessary.
            if encoding:
                if isinstance(source, bytes):
                    source = source.decode(encoding)
                source = source.encode('ascii', 'replace')

            # Parse a string buffer from memory.
            with misc_utils.log_time('fava.parser.parser.parse_string',
                                        log_timings, indent=2):
                (src_entries,
                 src_errors,
                 src_options_map) = parser.parse_string(source)

            # Merge the entries resulting from the parsed file.
            entries.extend(src_entries)
            parse_errors.extend(src_errors)

            # We need the options from the very top file only (the very
            # first file being processed). No merging of options should
            # occur.
            if is_top_level:
                options_map = src_options_map
            else:
                aggregate_options_map(options_map, src_options_map)

            # Add includes to the list of sources to process.
            for include_filename in src_options_map['include']:
                if include_filename.startswith("gs://"):
                    pass
                elif include_filename.startswith("/"):
                    include_filename = "gs://" + bucket_name + "/" + include_filename
                else:
                    if cwd is None:
                        parse_errors.append(
                            LoadError(data.new_metadata("<load>", 0),
                                      'File "{}" is a relative path included in a string'.format(
                                          include_filename), None))
                    include_filename = cwd + '/' + include_filename

                parts = include_filename.split('/')
                new_parts = []
                for p in parts:
                    if p == '.':
                        continue
                    elif p == '..':
                        if len(new_parts) > 0:
                            new_parts.pop()
                    else:
                        new_parts.append(p)
                include_filename = '/'.join(new_parts)

                # TODO: glob pattern matching

                # Add the include filenames to be processed later.
                source_stack.append((include_filename, True))

    # Make sure we have at least a dict of valid options.
    if options_map is None:
        options_map = options.OPTIONS_DEFAULTS.copy()

    # Save the set of parsed filenames in options_map.
    options_map['include'] = sorted(filenames_seen)

    return entries, parse_errors, options_map

def _load_gcs(sources, log_timings, extra_validations, encoding):
    """Parse Beancount input, run its transformations and validate it.

    (This is an internal method.)
    This routine does all that is necessary to obtain a list of entries ready
    for realization and working with them. This is the principal call for of the
    scripts that load a ledger. It returns a list of entries transformed and
    ready for reporting, a list of errors, and parser's options dict.

    Args:
      sources: A list of (filename-or-string, is-filename) where the first
        element is a string, with either a filename or a string to be parsed directly,
        and the second arugment is a boolean that is true if the first is a filename.
        You may provide a list of such arguments to be parsed. Filenames must be absolute
        paths.
      log_timings: A file object or function to write timings to,
        or None, if it should remain quiet.
      extra_validations: A list of extra validation functions to run after loading
        this list of entries.
      encoding: A string or None, the encoding to decode the input filename with.
    Returns:
      See load() or load_string().
    """
    assert isinstance(sources, list) and all(isinstance(el, tuple) for el in sources)

    if hasattr(log_timings, 'write'):
        log_timings = log_timings.write

    # Parse all the files recursively.
    entries, parse_errors, options_map = _parse_recursive_gcs(sources, log_timings, encoding)

    # Ensure that the entries are sorted before running any processes on them.
    entries.sort(key=data.entry_sortkey)

    # Run interpolation on incomplete entries.
    entries, balance_errors = booking.book(entries, options_map)
    parse_errors.extend(balance_errors)

    # Transform the entries.
    entries, errors = run_transformations(entries, parse_errors, options_map, log_timings)

    # Validate the list of entries.
    with misc_utils.log_time('beancount.ops.validate', log_timings, indent=1):
        valid_errors = validation.validate(entries, options_map, log_timings,
                                           extra_validations)
        errors.extend(valid_errors)

        # Note: We could go hardcore here and further verify that the entries
        # haven't been modified by user-provided validation routines, by
        # comparing hashes before and after. Not needed for now.

    # Compute the input hash.
    options_map['input_hash'] = compute_input_hash(options_map['include'])

    return entries, errors, options_map

def load_gcs(filename, log_timings=None, log_errors=None, extra_validations=None,
             encoding=None):
    """Open a Beancount input file, parse it, run transformations and validate.

    Args:
      filename: The name of the file to be parsed.
      log_timings: A file object or function to write timings to,
        or None, if it should remain quiet.
      log_errors: A file object or function to write errors to,
        or None, if it should remain quiet.
      extra_validations: A list of extra validation functions to run after loading
        this list of entries.
      encoding: A string or None, the encoding to decode the input filename with.
    Returns:
      A triple of (entries, errors, option_map) where "entries" is a date-sorted
      list of entries from the file, "errors" a list of error objects generated
      while parsing and validating the file, and "options_map", a dict of the
      options parsed from the file.
    """

    entries, errors, options_map = _load_gcs(
        filename, log_timings,
        extra_validations, encoding)
    _log_errors(errors, log_errors)
    return entries, errors, options_map


class GCSModule(FavaModule):
    """Functions related to reading/writing to Beancount files."""

    def list_sources(self):
        """List source files.

        Returns:
            A list of all sources files, with the main file listed first.

        """
        main_file = self.ledger.beancount_file_path
        return [main_file] + \
            sorted(filter(
                lambda x: x != main_file,
                [os.path.join(
                    os.path.dirname(main_file), filename)
                 for filename in self.ledger.options['include']]))

    def get_source(self, path):
        """Get source files.

        Args:
            path: The path of the file.

        Returns:
            A string with the file contents and the `sha256sum` of the file.

        Raises:
            FavaAPIException: If the file at `path` is not one of the
                source files.

        """
        if path not in self.list_sources():
            raise FavaAPIException('Trying to read a non-source file')

        with open(path, mode='rb') as file:
            contents = file.read()

        sha256sum = sha256(contents).hexdigest()
        source = codecs.decode(contents)

        return source, sha256sum

    def set_source(self, path, source, sha256sum):
        """Write to source file.

        Args:
            path: The path of the file.
            source: A string with the file contents.
            sha256sum: Hash of the file.

        Returns:
            The `sha256sum` of the updated file.

        Raises:
            FavaAPIException: If the file at `path` is not one of the
                source files or if the file was changed externally.

        """
        _, original_sha256sum = self.get_source(path)
        if original_sha256sum != sha256sum:
            raise FavaAPIException('The file changed externally.')

        contents = codecs.encode(source)
        with open(path, 'w+b') as file:
            file.write(contents)

        self.ledger.extensions.run_hook('after_write_source', path, source)
        self.ledger.load_file()

        return sha256(contents).hexdigest()

    def insert_metadata(self, entry_hash, basekey, value):
        """Insert metadata into a file at lineno.

        Also, prevent duplicate keys.
        """
        self.ledger.changed()
        entry = self.ledger.get_entry(entry_hash)
        key = next_key(basekey, entry.meta)
        insert_metadata_in_file(entry.meta['filename'],
                                entry.meta['lineno'] - 1, key, value)
        self.ledger.extensions.run_hook('after_insert_metadata', entry, key,
                                        value)

    def insert_entries(self, entries):
        """Insert entries.

        Args:
            entries: A list of entries.

        """
        self.ledger.changed()
        for entry in sorted(entries, key=incomplete_sortkey):
            insert_entry(entry, self.list_sources(), self.ledger.fava_options)
            self.ledger.extensions.run_hook('after_insert_entry', entry)

    def render_entries(self, entries):
        """Return entries in Beancount format.

        Only renders Balances and Transactions.

        Args:
            entries: A list of entries.

        Yields:
            The entries rendered in Beancount format.

        """
        excl_flags = [
            flags.FLAG_PADDING,      # P
            flags.FLAG_SUMMARIZE,    # S
            flags.FLAG_TRANSFER,     # T
            flags.FLAG_CONVERSIONS,  # C
            flags.FLAG_UNREALIZED,   # U
            flags.FLAG_RETURNS,      # R
            flags.FLAG_MERGING,      # M
        ]

        for entry in entries:
            if isinstance(entry, (data.Balance, data.Transaction)):
                if isinstance(entry, data.Transaction) and \
                   entry.flag in excl_flags:
                    continue
                try:
                    yield get_entry_slice(entry)[0] + '\n'
                except FileNotFoundError:
                    yield _format_entry(entry, self.ledger.fava_options)


def incomplete_sortkey(entry):
    """Sortkey for entries that might have incomplete metadata."""
    return (entry.date, data.SORT_ORDER.get(type(entry), 0))


def next_key(basekey, keys):
    """Returns the next unused key for basekey in the supplied array.

    The first try is `basekey`, followed by `basekey-2`, `basekey-3`, etc
    until a free one is found.
    """
    if basekey not in keys:
        return basekey
    i = 2
    while '{}-{}'.format(basekey, i) in keys:
        i = i + 1
    return '{}-{}'.format(basekey, i)


def leading_space(line):
    """Returns a string representing the leading whitespace for the specified
    string."""
    return line[:len(line) - len(line.lstrip())]


def insert_metadata_in_file(filename, lineno, key, value):
    """Inserts the specified metadata in the file below lineno, taking into
    account the whitespace in front of the line that lineno."""
    with open(filename, "r") as file:
        contents = file.readlines()

    # use the whitespace of the following line, else use double the whitespace
    indention = leading_space(contents[lineno + 1])

    contents.insert(lineno + 1, '{}{}: "{}"\n'.format(indention, key, value))

    with open(filename, "w") as file:
        contents = "".join(contents)
        file.write(contents)


def find_entry_lines(lines, lineno):
    """Lines of entry starting at lineno."""
    entry_lines = [lines[lineno]]
    while True:
        lineno += 1
        try:
            line = lines[lineno]
        except IndexError:
            break
        if not line.strip() or re.match('[0-9a-z]', line[0]):
            break
        entry_lines.append(line)
    return entry_lines


def get_entry_slice(entry):
    """Get slice of the source file for an entry.

    Args:
        entry: An entry.

    Returns:
        A string containing the lines of the entry and the `sha256sum` of
        these lines.

    Raises:
        FavaAPIException: If the file at `path` is not one of the
            source files.

    """
    with open(entry.meta['filename'], mode='r') as file:
        lines = file.readlines()

    entry_lines = find_entry_lines(lines, entry.meta['lineno'] - 1)
    entry_source = ''.join(entry_lines).rstrip('\n')
    sha256sum = sha256(codecs.encode(entry_source)).hexdigest()

    return entry_source, sha256sum


def save_entry_slice(entry, source_slice, sha256sum):
    """Save slice of the source file for an entry.

    Args:
        entry: An entry.
        source_slice: The lines that the entry should be replaced with.
        sha256sum: The sha256sum of the current lines of the entry.

    Returns:
        The `sha256sum` of the new lines of the entry.

    Raises:
        FavaAPIException: If the file at `path` is not one of the
            source files.

    """

    with open(entry.meta['filename'], 'r') as file:
        lines = file.readlines()

    first_entry_line = entry.meta['lineno'] - 1
    entry_lines = find_entry_lines(lines, first_entry_line)
    entry_source = ''.join(entry_lines).rstrip('\n')
    original_sha256sum = sha256(codecs.encode(entry_source)).hexdigest()
    if original_sha256sum != sha256sum:
        raise FavaAPIException('The file changed externally.')

    lines = (lines[:first_entry_line]
             + [source_slice + '\n']
             + lines[first_entry_line + len(entry_lines):])
    with open(entry.meta['filename'], "w") as file:
        file.writelines(lines)

    return sha256(codecs.encode(source_slice)).hexdigest()


def insert_entry(entry, filenames, fava_options):
    """Insert an entry.

    Args:
        entry: An entry.
        filenames: List of filenames.
        fava_options: The ledgers fava_options. Note that the line numbers of
            the insert options might be updated.
    """
    insert_options = fava_options.get('insert-entry', [])
    if isinstance(entry, data.Transaction):
        accounts = reversed([p.account for p in entry.postings])
    else:
        accounts = [entry.account]
    filename, lineno = find_insert_position(accounts, entry.date,
                                            insert_options, filenames)
    content = _format_entry(entry, fava_options) + '\n'

    with open(filename, "r") as file:
        contents = file.readlines()

    contents.insert(lineno, content)

    with open(filename, "w") as file:
        file.writelines(contents)

    for index, option in enumerate(insert_options):
        added_lines = content.count('\n') + 1
        if option.filename == filename and option.lineno > lineno:
            insert_options[index] = option._replace(
                lineno=lineno + added_lines)


def _format_entry(entry, fava_options):
    """Wrapper that strips unnecessary whitespace from format_entry."""
    string = align(format_entry(entry), fava_options)
    return '\n'.join((line.rstrip() for line in string.split('\n')))


def find_insert_position(accounts, date, insert_options, filenames):
    """Find insert position for an account.

    Args:
        accounts: A list of accounts.
        date: A date. Only InsertOptions before this date will be considered.
        insert_options: A list of InsertOption.
        filenames: List of Beancount files.
    """
    position = None

    for account in accounts:
        for insert_option in insert_options:
            if insert_option.date >= date:
                break
            if insert_option.re.match(account):
                position = (insert_option.filename, insert_option.lineno - 1)

    if not position:
        position = filenames[0], len(open(filenames[0]).readlines()) + 1

    return position
