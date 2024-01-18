#!/bin/python3
import argparse
import redis
import os
import shutil
import base64

class StringTrie:

    key = ""
    first = 0
    last = 0

    def __init__(self, key, first, last):
        self.key = key
        self.first = first
        self.last = last
        self.children = list()

    def isLeaf(self):
        return self.last - self.first == 1

    def toStr(self, level=0):
        result = (" "*(1*level)) + "'" + str(self.key) + "'" + " [" + str(self.first)+","+str(self.last)+")\n"
        for c in self.children:
            result += c.toStr(level+1)
        return result

    def __str__(self):
        print(self.key)


def create_trie(string_list):
    ssl = list(sorted(string_list))
    root = StringTrie("", 0, len(ssl))
    _create_trie(ssl, root)
    return root

def _create_trie(ssl, parent):

    current = parent.first
    for i in range(parent.first+1, parent.last):
        if len(longest_common_prefix([ssl[current], ssl[i]])) - len(parent.key) == 0:
            if i-1 != current:
                trie = StringTrie(ssl[current][0:len(longest_common_prefix([ssl[current], ssl[i-1]]))], current, i)
                parent.children.append(trie)
                _create_trie(ssl, trie)
            else:
                trie = StringTrie(ssl[current], current, i)
                parent.children.append(trie)
            current = i
    if parent.last-1 != current:
        if parent.first == current and len(longest_common_prefix([ssl[current], ssl[parent.last-1]])) - len(parent.key) == 0:
            for i in range(parent.first, parent.last):
                trie = StringTrie(ssl[i], i, i+1)
                parent.children.append(trie)
        else:
            trie = StringTrie(longest_common_prefix([ssl[current], ssl[parent.last-1]]), current, parent.last)
            parent.children.append(trie)
            _create_trie(ssl, trie)
    else:
        trie = StringTrie(ssl[current], current, current+1)
        parent.children.append(trie)


def load_data(folder, host, port, unix_socket_path, password, db, keys, use_expireat, empty):
    connection = count_key_types(host, port, unix_socket_path, password, db, keys)
    if not os.path.exists(folder):
        print("The specified folder "+folder+" does not exist.")
        exit()
    if (empty):
        connection.flushdb()
    for root, dirs, files in os.walk(folder, topdown=False):
        for name in files:
            file = os.path.join(root, name)
            restored_encoded_key = file[len(folder):].replace(os.sep,"")
            restored_key = base64.urlsafe_b64decode(restored_encoded_key.encode('utf-8'))
            #print("'"+file+"' -> "+str(restored_key)+"")
            try:
                with open(file, 'r') as f:
                    lines = f.readlines()
                    lines = [x.strip() for x in lines]
                    key_type = lines[0]
                    expire = int(lines[1])
                    if key_type == "string":
                        if len(lines) < 3:
                            lines.append("")
                        connection.set(restored_key, base64.urlsafe_b64decode(lines[2]))
                    elif key_type == "hash":
                        for i in range(2, len(lines)-1, 2):
                            connection.hset(restored_key, base64.urlsafe_b64decode(lines[i]), base64.urlsafe_b64decode(lines[i+1]))
                    elif key_type == "list":
                        for i in range(2, len(lines)-1):
                            connection.rpush(restored_key, base64.urlsafe_b64decode(lines[i]))
                    elif key_type == "set":
                        for i in range(2, len(lines)-1):
                            connection.sadd(restored_key, base64.urlsafe_b64decode(lines[i]))
                    elif key_type == "zset":
                        for i in range(2, len(lines)-1):
                            connection.zadd(restored_key, {base64.urlsafe_b64decode(lines[i]): float(lines[i+1])})
                    elif key_type == "stream":
                        i = 2
                        while i < len(lines) - 1:
                            id = base64.urlsafe_b64decode(lines[i])
                            d_size = int(lines[i+1])
                            i += 2
                            d_end = i + 2*d_size
                            d = dict()
                            while i < d_end:
                                d[base64.urlsafe_b64decode(lines[i])] = base64.urlsafe_b64decode(lines[i+1])
                                i += 2
                            connection.xadd(restored_key, d, id)
                    if expire != -1:
                        connection.expire(restored_key, expire)
            except Exception as e:
                print(e)
                

def store_data(folder, host, port, unix_socket_path, password, db, keys, use_expireat, empty):
    # Implement the logic to store data from folder structure to Redis
    connection = count_key_types(host, port, unix_socket_path, password, db, keys)
    all_keys = connection.keys("*" if keys is None else keys)
    all_base64_keys = list()
    for key in all_keys:
        all_base64_keys.append(base64.urlsafe_b64encode(key).decode('utf-8'))

    trie = create_trie(all_base64_keys)

    if empty:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            os.mkdir(folder)


    # Create folders and files based on the trie
    create_folders_and_files(connection, trie, current_path=folder)


def encodeData(connection, key):
    key_type = connection.type(key).decode('utf-8')
    result = key_type + "\n" + str(connection.ttl(key))+"\n"
    if key_type == "string":
        result += base64.urlsafe_b64encode(connection.get(key)).decode('utf-8')
    elif key_type == "hash":
        d = connection.hgetall(key)
        for k in d.keys():
            v = d[k]
            result += base64.urlsafe_b64encode(k).decode('utf-8')+"\n"
            result += base64.urlsafe_b64encode(v).decode('utf-8')+"\n"
    elif key_type == "list":
        for v in connection.lrange(key, 0, -1):
            result += base64.urlsafe_b64encode(v).decode('utf-8')+"\n"
    elif key_type == "zset":
        for value, score in connection.zrange(key, 0, -1, withscores=True):
            result += base64.urlsafe_b64encode(value).decode('utf-8')+"\n"
            result += str(score)+"\n"
    elif key_type == "set":
        for value in connection.smembers(key):
            result += base64.urlsafe_b64encode(value).decode('utf-8')+"\n"
    elif key_type == "stream":
        l = connection.xrange(key)
        for id, d in l:
            result += base64.urlsafe_b64encode(id).decode('utf-8')+"\n"
            result += str(len(d))+"\n"
            for k in d.keys():
                result += base64.urlsafe_b64encode(k).decode('utf-8')+"\n"
                result += base64.urlsafe_b64encode(d[k]).decode('utf-8')+"\n"
    return result


def create_folders_and_files(connection, trie, current_path=""):
    for child in trie.children:
        child_name = child.key[len(trie.key):]
        if len(child_name) == 0:
            child_name = " "
        child_path = os.path.join(current_path, child_name)
        if child.isLeaf():
            with open(child_path, 'w') as f:
                f.write(encodeData(connection, base64.urlsafe_b64decode(child.key)))
        else:
            os.makedirs(child_path, exist_ok=True)  # Use exist_ok to avoid raising an error if the folder already exists
            create_folders_and_files(connection, child, child_path)


def longest_common_prefix(strs):
    l = len(strs[0])
    for i in range(1, len(strs)):
        length = min(l, len(strs[i]))
        while length > 0 and strs[0][0:length] != strs[i][0:length]:
            length = length - 1
            if length == 0:
                return ""
    return strs[0][0:length]


def count_key_types(host, port, unix_socket_path, password, db, keys):
    if unix_socket_path:
        connection = redis.StrictRedis(unix_socket_path=unix_socket_path, db=db)
    else:
        connection = redis.StrictRedis(host=host, port=port, db=db)

    # Authenticate if a password is provided
    if password:
        connection.auth(password)

    # Get all keys in the database
    all_keys = connection.keys('*' if keys is None else keys)

    # Count the number of each key type
    key_type_count = {}
    for key in all_keys:
        key_type = connection.type(key).decode('utf-8')
        key_type_count[key_type] = key_type_count.get(key_type, 0) + 1

    # Print the result
    print("Key Type Counts:")
    for key_type, count in key_type_count.items():
        print(f"{key_type}: {count}")
    return connection

def main():
    parser = argparse.ArgumentParser(description='Redis Text Backup Utility')

    parser.add_argument('command', choices=['load', 'store', 'help'], help='Command to execute')
    parser.add_argument('folder', help='Folder location for storing/loading data')

    common_args = parser.add_argument_group('Common Arguments')
    common_args.add_argument('--host', default="localhost", help='Host name or IP address for Redis server')
    common_args.add_argument('--port', default=6379, type=int, help='Port number for Redis server')
    common_args.add_argument('--unix_socket_path', help='Path to the Unix socket for Redis connection')
    common_args.add_argument('--password', help='Password for Redis connection')
    common_args.add_argument('--db', default=0, type=int, help='Redis database to connect to')
    common_args.add_argument('--empty', default=False, action='store_true', help='Empty the datbase/folderbefore loading/storing the data')

    load_args = parser.add_argument_group('Load-specific Arguments')
    load_args.add_argument('--use_expireat', action='store_true', help='(Missing) Use expireat instead of ttl when loading expiring keys')

    store_args = parser.add_argument_group('Store-specific Arguments')
    store_args.add_argument('--keys', help='Only dump keys matching the specified pattern. Default: *')

    args = parser.parse_args()

    if args.command == 'help':
        parser.print_help()
    else:
        if not args.host or not args.port:
            parser.error('Host and port are required for Redis connection.')

        if args.command == 'load':
            load_data(args.folder, args.host, args.port, args.unix_socket_path, args.password, args.db, args.keys, args.use_expireat, args.empty)
        elif args.command == 'store':
            store_data(args.folder, args.host, args.port, args.unix_socket_path, args.password, args.db, args.keys, args.use_expireat, args.empty)

if __name__ == '__main__':
    main()
