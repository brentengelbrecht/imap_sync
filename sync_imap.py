import imaplib
import email
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
flag_dryrun = True
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
        CREATE TABLE messages (
            r_id INTEGER PRIMARY KEY,
            u_id INTEGER NOT NULL,
            gm_id TEXT,
            synced BOOLEAN,
            errored BOOLEAN,
            folder TEXT,
            filename TEXT,
            err_message TEXT,
            user_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(r_id),
            UNIQUE(folder, u_id) ON CONFLICT ABORT
        )
    ''')
    cursor.connection.commit()

def update_db_reset_sync_flags():
    global cursor
    cursor.execute("UPDATE messages SET synced = false, errored = false, filename = null, err_message = null")
    cursor.connection.commit()

def get_db_msg_count(folder):
    global cursor
    cursor.execute("SELECT COUNT(*) FROM messages WHERE folder = '{}'".format(folder.strip('"\'')))
    count = cursor.fetchone()
    return count[0]

def get_db_userid(user_name):
    global cursor
    cursor.execute("SELECT r_id FROM users WHERE username = '{}'".format(user_name))
    r_id = cursor.fetchone()
    return r_id[0]

def update_db_username(user_name):
    global cursor
    cursor.execute("INSERT INTO users (username) VALUES ('{}') ON CONFLICT (username) DO NOTHING".format(user_name))
    cursor.connection.commit()
    r_id = cursor.lastrowid
    if r_id == 0:
        cursor.execute("SELECT r_id FROM users WHERE username = '{}'".format(user_name))
        r_id = cursor.fetchone()[0]
    return r_id

def update_db_msg_id(usr_id, folder, seq_id, u_id, gm_id):
    global cursor
    uid = u_id.decode('utf-8')
    fld = folder.strip('"\'')
    cursor.execute("INSERT INTO messages (folder, u_id, gm_id, user_id, synced) VALUES ('{}', '{}', {}, {}, false)".format(fld, uid, gm_id, usr_id))

def update_db_errored(usr_id, folder, err_msg, u_id, gm_id):
    global cursor
    fld = folder.strip('"\'')
    cursor.execute("UPDATE messages SET errored = true, synced = true, err_message = '{}' WHERE (user_id = {} AND folder = '{}') AND (gm_id = '{}' OR u_id = {})".format(err_msg, usr_id, fld, gm_id, u_id))
    cursor.connection.commit()

def update_db_downloaded(usr_id, folder, file_name, u_id, gm_id):
    global cursor
    fld = folder.strip('"\'')
    cursor.execute("UPDATE messages SET synced = true, filename = '{}' WHERE (user_id = {} AND folder = '{}') AND (gm_id = '{}' OR u_id = {})".format(file_name, usr_id, fld, gm_id, u_id))
    cursor.connection.commit()

def get_db_next_unsynced(folder, page_size = 30):
    global cursor
    cursor.execute("SELECT r_id, u_id, gm_id FROM messages WHERE folder = '{}' AND synced = false LIMIT {}".format(folder.strip('"\''), page_size))
    return cursor.fetchall()

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
        mail.select('inbox')
    except Exception as e:
        print('[mailbox_login] Connection failed: {}'.format(e))
        raise

    if mail:
        print('Logged in to imap server: {} user: {}\n'.format(host, user))

    return mail

def mailbox_logout(mail):
    mail.close()
    mail.logout()
    print('\nLogged out of imap server\n')

def fetch_one_mail(mail, u_id, gm_id):
    res, seq_id = mail.uid('search', None, str(u_id))
    try:
        res, buffer = mail.uid('fetch', seq_id[0], '(RFC822)')
    except Exception:
        print('[fetch_one_mail] Cannot retrieve message from mailbox, u_id {}'.format(u_id))
        return 'NO', None

    #message = email.message_from_bytes(buffer[0][1])
    if res != 'OK':
        print('[fetch_one_mail] Could not fetch message for unique id : {}'.format(u_id))
    return res, buffer

def unpack_fetch_unique(data):
    tag = 'X-GM-MSGID'

    asc = data[0].decode('utf-8')
    seq, ustr = asc.split(' ', 1)
    _, gid, _, uid = ustr.strip(')(').split()

    return int(seq), int(uid), int(gid)

def sync_db_unique_ids(user_rid, mail, folder, msg_nums):
    global cursor

    read_size = 0
    i = 0
    j = 0
    for num in msg_nums[0].split():
        i += 1
        #res, data = mail.fetch(num, '(X-GM-MSGID)')
        res, data = mail.uid('fetch', num, '(X-GM-MSGID)')
        read_size += len(data[0])
        if res != 'OK':
            print('[sync_db_unique_ids] Could not get message unique id for message: {}'.format(num))
        else:
            seq_id, u_id, gm_id = unpack_fetch_unique(data)
            update_db_msg_id(user_rid, folder, seq_id, num, gm_id)

        if i >= 50:
            j += i
            i = 0
            cursor.connection.commit()
            if flag_verbose:
                print('[sync_db_unique_ids] Updated id\'s [{}] data read [{:,} bytes]'.format(j, read_size))

    cursor.connection.commit()
    j += i
    print('[sync_db_unique_ids] Updated id\'s [{}] data read [{:,} bytes]'.format(j, read_size))
    print('[sync_db_unique_ids] Complete\n')
    return read_size

def sync_email_ids(host, user, password, imap_folder = '"[Gmail]/All Mail"'):
    mail = mailbox_login(host, user, password)

    folders = get_mail_folders(mail)
    num_ids = 0
    read_size = sys.getsizeof(folders)
    if not has_imap_folder(folders, imap_folder):
        print('[sync_email_ids] {} folder not found'.format(imap_folder))
        return

    resp, msg_num = mail.select(imap_folder)
    if resp != 'OK':
        print('[sync_email_ids] Could not select folder: {}'.format(imap_folder))
    else:
        read_size = len(msg_num[0])
        mailbox_count = int(msg_num[0].decode('utf-8'))
        print('\nSelected folder: {} message count: {}\n'.format(imap_folder, mailbox_count))

        if get_db_msg_count(imap_folder) != mailbox_count:
            print('[sync_email_ids] Syncing message id\'s\n')
            r_id = update_db_username(user)

            #status, msg_nums = mail.search(None, 'ALL')  # sequence numbers
            status, msg_nums = mail.uid('search', None, 'ALL')  # UIDs
            read_size += sync_db_unique_ids(r_id, mail, imap_folder, msg_nums)
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
        name = message_id + '.txt'
    return name

def unpack_info_string(data):
    asc = data.decode('utf-8')
    ustr = asc.split()
    # '6 (UID 1088 RFC822 {6453}'
    seq, _, uid, _, size = ustr
    size = size.strip('{}')
    return int(seq), int(uid), int(size)

def sync_emails(mail, user_name, mailbox_path, batch):
    if mail is None:
        print('[sync_emails] No open mailbox')
        return

    usr_id = get_db_userid(user_name)
    if flag_verbose:
        print()

    read_size = 0
    write_size = 0
    message_count = 0
    for row in batch:
        seq_id, u_id, gm_id = row
        res, data = fetch_one_mail(mail, u_id, gm_id)
        if res != 'OK':
            continue

        read_size += len(data[0][0]) + len(data[0][1])
        seq_id, u_id, size = unpack_info_string(data[0][0])

        try:
            email = data[0][1].decode('utf-8')
        except UnicodeDecodeError as e:
            print('[sync_emails] Could not decode email, u_id {} size {:,} bytes'.format(u_id, size))
            update_db_errored(usr_id, folder, e.reason, u_id, gm_id)
            continue

        matches = re.search('\r\nMessage-Id: (.*)\r\n', email, re.IGNORECASE)
        if matches is not None:
            msg_id = matches.group(1)
            file_name = generate_email_filename(mailbox_path, msg_id)
        else:
            file_name = generate_email_filename(mailbox_path, str(time.time()) + '.' + str(random.randint(1, 999999)))

        write_to_file('{}/{}'.format(mailbox_path, file_name), email, flag_dryrun)
        update_db_downloaded(usr_id, folder, file_name, u_id, gm_id)
        write_size += size
        message_count += 1

        if flag_verbose:
            print('[sync_emails] Wrote file: name [{}] size: [{:,} bytes]'.format(file_name, size))

    if flag_verbose:
        print()
    print('[sync_emails] messages [{:,}] data read [{:,} bytes] data written [{:,} bytes]'.format(message_count, read_size, write_size))
    return message_count, read_size, write_size

def sync_mailbox(page_size):
    ttl_m, ttl_r, ttl_w = 0, 0, 0

    mail = mailbox_login(host, username, password)
    batch = get_db_next_unsynced(folder, page_size)
    while len(batch) > 0:
        message_count, read_size, write_size = sync_emails(mail, username, mailbox_path, batch)
        ttl_m += message_count
        ttl_r += read_size
        ttl_w += write_size
        batch = get_db_next_unsynced(folder, page_size)
        if flag_verbose:
            print('[sync_mailbox] Progress: message count [{:,}] data read [{:,} bytes] data written [{:,}]'.format(ttl_m, ttl_r, ttl_w))

    mailbox_logout(mail)

    if flag_verbose:
        print()
    print('[sync_mailbox] Completed: message count [{:,}] data read [{:,} bytes] data written [{:,}]'.format(ttl_m, ttl_r, ttl_w))

if __name__ == '__main__':
    host = os.getenv('HOST')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    db_loc = os.getenv('DB_LOCATION')

    database = db_loc + '/' + db_name

    if os.path.isfile(database):
        connect_db(database)
    else:
        connect_db(database)
        create_db()

    if flag_reset_synced:
        update_db_reset_sync_flags()

    folder = '"[Gmail]/All Mail"'
    sync_email_ids(host, username, password, folder)
    mailbox_path = create_user_mailbox('data', username)
    sync_mailbox(default_page_size)
