import imaplib
import re
import logging
import sys
import os
import time
import random
import sqlite3

db_name = 'sync.db'
cursor = None
flag_verbose = False
flag_dryrun = False
flag_reset_synced = False
default_page_size = 20


def connect_db(db_name):
    global cursor
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()


def create_db():
    global cursor
    cursor.execute('PRAGMA foreign_keys = 1')
    cursor.execute('''
        CREATE TABLE users (
            r_id INTEGER PRIMARY KEY,
            username TEXT UNIQUE)
    ''')
    cursor.execute('''
        CREATE TABLE folders (
            r_id INTEGER PRIMARY KEY,
            name TEXT,
            ui_validity TEXT,
            user_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(r_id),
            UNIQUE(user_id, name, ui_validity) ON CONFLICT IGNORE)
    ''')
    cursor.execute('''
        CREATE TABLE messages (
            r_id INTEGER PRIMARY KEY,
            u_id INTEGER NOT NULL,
            gm_id TEXT,
            synced BOOLEAN,
            errored BOOLEAN,
            folder_id INTEGER,
            filename TEXT,
            err_message TEXT,
            user_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(r_id),
            FOREIGN KEY(folder_id) REFERENCES folders(r_id),
            UNIQUE(folder_id, u_id) ON CONFLICT ABORT
        )
    ''')
    cursor.connection.commit()

def update_db_reset_sync_flags():
    global cursor
    cursor.execute("UPDATE messages SET synced = false, errored = false, filename = null, err_message = null")
    cursor.connection.commit()

def update_db_clear_error_flags(fld_id):
    global cursor
    cursor.execute("UPDATE messages SET synced = false, errored = false, filename = null, err_message = null WHERE folder_id = {} AND errored = true".format(fld_id))
    cursor.connection.commit()

def get_db_msg_count(fld_id):
    global cursor
    cursor.execute("SELECT COUNT(*) FROM messages WHERE folder_id = {}".format(fld_id))
    count = cursor.fetchone()
    return count[0]

def get_db_userid(user_name):
    global cursor
    cursor.execute("SELECT r_id FROM users WHERE username = '{}'".format(user_name))
    r_id = cursor.fetchone()
    return r_id[0]

def get_db_folder_id(usr_id, folder, ui_validity = 1):
    global cursor
    fld = folder.strip('"\'')
    cursor.execute("SELECT r_id FROM folders WHERE user_id = {} AND name = '{}' AND ui_validity = {}".format(usr_id, fld, ui_validity))
    r_id = cursor.fetchone()
    return r_id[0]

def get_db_imap_folder_set(usr_id):
    global cursor
    cursor.execute("SELECT r_id, name, ui_validity FROM folders WHERE user_id = {}".format(usr_id))
    return cursor.fetchall()

def update_db_username(user_name):
    global cursor
    cursor.execute("INSERT INTO users (username) VALUES ('{}') ON CONFLICT (username) DO NOTHING".format(user_name))
    cursor.connection.commit()
    r_id = cursor.lastrowid
    if r_id == 0:
        r_id = get_db_userid(user_name)
    return r_id

def update_db_folder(usr_id, folder, ui_validity = 1):
    global cursor
    fld = folder.strip('"\'')
    cursor.execute("INSERT INTO folders (user_id, name, ui_validity) VALUES ({}, '{}', {})".format(usr_id, fld, ui_validity))
    cursor.connection.commit()
    r_id = cursor.lastrowid
    if r_id == 0:
        r_id = get_db_folder_id(usr_id, folder, ui_validity)
    return r_id

def update_db_msg_id(usr_id, fld_id, u_id, gm_id):
    global cursor
    if isinstance(u_id, int):
        num = u_id
    else:
        try:
            num = int(u_id.decode('utf-8'))
        except (UnicodeDecodeError, AttributeError):
            raise
    cursor.execute("INSERT INTO messages (synced, folder_id, user_id, u_id, gm_id) VALUES (false, {}, {}, {}, '{}')".format(fld_id, usr_id, num, gm_id))
    cursor.connection.commit()

def update_db_errored(usr_id, fld_id, err_msg, u_id, gm_id):
    global cursor
    cursor.execute("UPDATE messages SET errored = true, synced = true, err_message = '{}' WHERE (user_id = {} AND folder_id = {}) AND (gm_id = '{}' OR u_id = {})".format(err_msg, usr_id, fld_id, gm_id, u_id))
    cursor.connection.commit()

def update_db_downloaded(usr_id, fld_id, file_name, u_id, gm_id):
    global cursor
    cursor.execute("UPDATE messages SET synced = true, errored = false, filename = '{}' WHERE (user_id = {} AND folder_id = {}) AND (gm_id = '{}' OR u_id = {})".format(file_name, usr_id, fld_id, gm_id, u_id))
    cursor.connection.commit()

def get_db_next_unsynced(fld_id, page_size = 30):
    global cursor
    cursor.execute("SELECT r_id, u_id, gm_id FROM messages WHERE folder_id = {} AND synced = false LIMIT {}".format(fld_id, page_size))
    return cursor.fetchall()

def get_mail_status(mail, imap_folder):
    return mail.status(imap_folder, '(MESSAGES UNSEEN RECENT)')

def get_mail_folders(mail):
    res, data = mail.list()
    if res != 'OK':
        logging.error('Could not get folders')

    result = ()
    print('Folder list:')
    for folder in data:
        item = folder.decode('utf-8')
        result += (item,)
        print('\t{}'.format(item))

    return result

def has_imap_folder(folders, target):
    for folder in folders:
        tok = folder.split(' "/" ')
        if tok[1].strip('"\'').lower() == target.strip('"\'').lower():
            return True
    return False

def create_user_mailbox(directory, user):
    user_name, domain = user.split('@')
    update_db_username(user)
    return create_mailbox_path('gmail', directory, domain + '/' + user_name)

def create_mailbox_path(type, path, folder):
    result = path
    if type == 'gmail':
        levels = folder.split('/')
        for level in levels:
            clevel = level.strip('"\'')
            result += '/' + clevel
            if not os.path.isdir(result):
                os.mkdir(result)
    return result

def mailbox_login(host, user, password):
    try:
        mail = imaplib.IMAP4_SSL(host)
        mail.login(user, password)
        mail.select('inbox', readonly=True)
    except Exception as e:
        print('[mailbox_login] Connection failed: {}'.format(e))
        raise

    if mail:
        print('[mailbox_login] Logged in to imap server: {} user: {}\n'.format(host, user))

    return mail

def mailbox_logout(mail):
    mail.close()
    mail.logout()
    print('\n[mailbox_logout] Logged out of imap server\n')

def fetch_one_mail(mail, u_id, gm_id):
    res, seq_id = mail.uid('search', None, str(u_id))
    #res, seq_id = mail.uid('search', f'(X-GM-MSGID {gm_id})')
    if len(seq_id[0]) == 0:
        if flag_verbose:
            print('[fetch_one_mail] Search for email unique_id failed [u_id {}]'.format(u_id))
        return 'NO', None

    try:
        res, buffer = mail.uid('fetch', seq_id[0], '(RFC822)')
    except Exception:
        if flag_verbose:
            print('[fetch_one_mail] Cannot retrieve message [u_id {}]'.format(u_id))
        return 'NO', None

    return res, buffer

def unpack_fetch_unique(data):
    # tag = 'X-GM-MSGID'
    asc = data[0].decode('utf-8')
    seq, ustr = asc.split(' ', 1)
    _, gid, _, uid = ustr.strip(')(').split()

    return int(seq), int(uid), int(gid)

def sync_db_unique_ids(user_rid, mail, folder_id, msg_nums):
    global cursor

    read_size, i, j = 0, 0, 0
    for uid in msg_nums[0].split():
        i += 1
        #res, data = mail.fetch(num, '(X-GM-MSGID)')
        res, data = mail.uid('fetch', uid, '(X-GM-MSGID)')
        read_size += len(data[0])
        if res != 'OK':
            print('[sync_db_unique_ids] Could not get message unique id for message: {}'.format(uid))
        else:
            seq_id, u_id, gm_id = unpack_fetch_unique(data)
            update_db_msg_id(user_rid, folder_id, uid, gm_id)

        if i >= 100:
            j += i
            i = 0
            cursor.connection.commit()
            print('[sync_db_unique_ids] Updated id\'s [{}] data read [{:,} bytes]'.format(j, read_size))

    cursor.connection.commit()
    j += i
    print('[sync_db_unique_ids] Updated id\'s [{}] data read [{:,} bytes]'.format(j, read_size))
    print('[sync_db_unique_ids] Complete\n')
    return read_size

def sync_email_ids(host, user, password, imap_folder, folder_id):
    mail = mailbox_login(host, user, password)

    ##abc = get_mail_status(mail, imap_folder) ## Get ui_validity here, and pass to update_db_folder()
    usr_id = get_db_userid(user)

    num_ids = 0
    resp, msg_num = mail.select('"{}"'.format(imap_folder), readonly=True)
    if resp != 'OK':
        print('[sync_email_ids] Could not select folder: {}'.format(imap_folder))
    else:
        read_size = len(msg_num[0])
        mailbox_count = int(msg_num[0].decode('utf-8'))
        print('\n[sync_email_ids] Selected folder [{}] message count [{:,}]\n'.format(imap_folder, mailbox_count))

        if get_db_msg_count(folder_id) != mailbox_count:
            print('[sync_email_ids] Syncing message id\'s\n')

            #status, msg_nums = mail.search(None, 'ALL')  # sequence numbers
            status, msg_nums = mail.uid('search', None, 'ALL')  # UIDs
            read_size += sync_db_unique_ids(usr_id, mail, folder_id, msg_nums)
            num_ids = len(msg_nums[0].split())

    mailbox_logout(mail)
    print('[sync_email_ids] id\'s [{:,}] data read [{:,} bytes]\n'.format(num_ids, read_size))

def write_to_file(path, data, dry_run=True):
    if not dry_run:
        with open(path, 'w') as text_file:
            text_file.write(data)
    return len(data)

def generate_email_filename(path, message_id):
    name = message_id + '.txt'
    while os.path.isfile(path + '/' + name):
        name = message_id + '.' + str(random.randint(1, 9999)) + '.txt'
    return name

def unpack_info_string(data):
    asc = data.decode('utf-8')
    ustr = asc.split()
    # '6 (UID 1088 RFC822 {6453}'
    seq, _, uid, _, size = ustr
    size = size.strip('{}')
    return int(seq), int(uid), int(size)

def sync_emails(mail, user_name, usr_id, folder, folder_id, mailbox_path, batch):
    if mail is None:
        print('[sync_emails] No open mailbox')
        return 0, 0, 0

    if flag_verbose:
        print()

    message_count, message_failed, read_size, write_size = 0, 0, 0, 0
    for row in batch:
        seq_id, u_id, gm_id = row
        res, data = fetch_one_mail(mail, u_id, gm_id)
        if res != 'OK':
            update_db_errored(usr_id, folder_id, 'SEARCH failed: u_id not found', u_id, gm_id)
            message_failed += 1
            continue

        read_size += len(data[0][0]) + len(data[0][1])
        seq_id, u_id, size = unpack_info_string(data[0][0])

        try:
            email = data[0][1].decode('utf-8')
        except UnicodeDecodeError as e:
            if flag_verbose:
                print('[sync_emails] Could not decode email, u_id {} size {:,} bytes'.format(u_id, size))
            update_db_errored(usr_id, folder_id, e.reason, u_id, gm_id)
            message_failed += 1
            continue

        matches = re.search('\r\nMessage-Id: (.*)\r\n', email, re.IGNORECASE)
        if matches is not None:
            msg_id = matches.group(1)
            file_name = generate_email_filename(mailbox_path, msg_id)
        else:
            file_name = generate_email_filename(mailbox_path, str(time.time()) + '.' + str(random.randint(1, 999999)))

        write_to_file('{}/{}'.format(mailbox_path, file_name), email, flag_dryrun)
        update_db_downloaded(usr_id, folder_id, file_name, u_id, gm_id)
        write_size += size
        message_count += 1

        if flag_verbose:
            print('[sync_emails] Wrote file: name [{}] size: [{:,} bytes]'.format(file_name, size))

    if flag_verbose:
        print()
    print('[sync_emails] Processed batch [success {:,}] [failed {:,}] data read [{:,} bytes] data written [{:,} bytes]'.format(message_count, message_failed, read_size, write_size))
    return message_count, read_size, write_size

def sync_mailbox(host, user, password, mailbox_path, folder, page_size):
    ttl_m, ttl_r, ttl_w = 0, 0, 0

    mail = mailbox_login(host, user, password)
    directory = create_mailbox_path('gmail', mailbox_path, folder)
    usr_id = get_db_userid(user)
    folder_id = get_db_folder_id(usr_id, folder)

    batch = get_db_next_unsynced(folder_id, page_size)
    while len(batch) > 0:
        message_count, read_size, write_size = sync_emails(mail, user, usr_id, folder, folder_id, directory, batch)
        ttl_m += message_count
        ttl_r += read_size
        ttl_w += write_size
        batch = get_db_next_unsynced(folder_id, page_size)
        if flag_verbose:
            print('[sync_mailbox] Progress: message count [{:,}] data read [{:,} bytes] data written [{:,} bytes]'.format(ttl_m, ttl_r, ttl_w))

    mailbox_logout(mail)

    if flag_verbose:
        print()
    print('[sync_mailbox] Completed: message count [{:,}] data read [{:,} bytes] data written [{:,} bytes]'.format(ttl_m, ttl_r, ttl_w))

def sync_imap_folder_names(host, user, password):
    mail = mailbox_login(host, user, password)

    usr_id = update_db_username(user)
    folders = get_mail_folders(mail)

    for folder in folders:
        data = folder
        flags = []
        folder_name = ''

        while data is not None:
            token = data.split(' ', 1)
            flag = token[0].strip('()')
            if flag.startswith('\\'):
                flags.append(flag)
            else:
                if flag != '"/"':
                    folder_name = data
                    break
            data = token[1]

        if '\\Noselect' not in flags:
            update_db_folder(usr_id, folder_name)

    mailbox_logout(mail)

if __name__ == '__main__':
    mailhost = os.getenv('MAILHOST')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')

    db_loc = os.getenv('DB_LOCATION')
    if db_loc:
        if not db_loc.endswith('/'):
            db_loc += '/'
        database = db_loc + db_name
    else:
        database = db_name

    if os.path.isfile(database):
        connect_db(database)
    else:
        connect_db(database)
        create_db()

    sync_imap_folder_names(mailhost, username, password)
    mailbox_path = create_user_mailbox('data', username)
    folder_list = get_db_imap_folder_set(get_db_userid(username))

    for folder in folder_list:
        folder_id, folder_name, _ = folder
        print('Processing folder: {}\n'.format(folder_name))

        if flag_reset_synced:
            update_db_reset_sync_flags()

        update_db_clear_error_flags(folder_id)

        sync_email_ids(mailhost, username, password, folder_name, folder_id)
        sync_mailbox(mailhost, username, password, mailbox_path, folder_name, 100)

