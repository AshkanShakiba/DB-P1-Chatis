import pickle
from redis import Redis
from threading import Thread
from datetime import datetime

rdc = Redis(host="localhost", port=32768, password="redispw")
pipeline = rdc.pipeline()
pubsub = rdc.pubsub()
exited = False


def store(key, obj):
    rdc.set(key, pickle.dumps(obj))


def load(key):
    return pickle.loads(rdc.get(key))


def pipeline_store(key, obj):
    pipeline.set(key, pickle.dumps(obj))
    pipeline.execute()


def receive():
    while not exited:
        message = pubsub.get_message()
        if message is not None and "type" in message and message["type"] == "message":
            print(message["data"].decode("utf-8"))


class Message:
    def __init__(self, sender, sent_at, text):
        self.sender = sender
        self.sent_at = sent_at
        self.text = text

    def get_dict(self):
        return {
            "sender": self.sender,
            "sent_at": self.sent_at,
            "text": self.text
        }


class Group:
    def __init__(self, name, creator, description, created_at, members):
        self.name = name
        self.creator = creator
        self.description = description
        self.created_at = created_at
        self.members = members

    def get_dict(self):
        return {
            "name": self.name,
            "creator": self.creator,
            "description": self.description,
            "created_at": self.created_at,
            "members": self.members
        }


def main():
    print("* Chatis - version 1.0 *")

    print("Your Username: ")
    username = input()
    joined_groups = []
    chatis_groups = load("chatis-groups")
    for group_name in chatis_groups:
        group_info = load(group_name)
        if username in group_info["members"]:
            joined_groups.append(group_info["name"])
            pubsub.subscribe(group_info["name"])

    receiver = Thread(target=receive)
    receiver.start()

    while True:
        print("[1] Send Message")
        print("[2] Join a New Group")
        print("[3] Create a New Group")
        print("[4] Exit")

        choice = int(input())

        if choice == 1:
            # create message
            index = 1
            for group_name in joined_groups:
                group_info = load(group_name)
                print(f"[{index}] "
                      + f"{group_info['name']}: {group_info['description']}, {len(group_info['members'])} members")
                index += 1
            if len(joined_groups) == 0:
                print("Error: there isn't any group")
                continue
            index_choice = int(input()) - 1
            if index_choice in range(0, len(joined_groups)):
                group_to_send = load(joined_groups[index_choice])
                print("Message: ")
                text = input()
                sent_at = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
                message = Message(sender=username, sent_at=sent_at, text=text)
                message_key = f"{group_to_send['name']}-{username}-{sent_at}"
                store(message_key, message.get_dict())
                # publish
                rdc.publish(group_to_send["name"], f"[{sent_at}] {username}@{group_to_send['name']}: {text}")
            else:
                print("Error: invalid input")

        elif choice == 2:
            index = 1
            chatis_groups = load("chatis-groups")
            for group_name in chatis_groups:
                group_info = load(group_name)
                print(f"[{index}] "
                      + f"{group_info['name']}: {group_info['description']}, {len(group_info['members'])} members")
                index += 1
            index_choice = int(input()) - 1
            if index_choice in range(0, len(chatis_groups)):
                # add member to group
                group_to_join = load(chatis_groups[index_choice])
                if username not in group_to_join["members"]:
                    group_to_join["members"].append(username)
                store(group_to_join["name"], group_to_join)
                if group_to_join["name"] not in joined_groups:
                    joined_groups.append(group_to_join["name"])
                # subscribe
                pubsub.subscribe(group_to_join["name"])
            else:
                print("Error: invalid input")

        elif choice == 3:
            # create group
            print("Name: ")
            name = input()
            if name in load("chatis-groups"):
                print("Error: this name is already taken")
                continue
            print("Description: ")
            description = input()
            created_at = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            members = [username]
            group = Group(name=name, creator=username, description=description, created_at=created_at, members=members)
            store(name, group.get_dict())
            # add group to all groups list
            chatis_groups = load("chatis-groups")
            chatis_groups.append(name)
            store("chatis-groups", chatis_groups)
            joined_groups.append(name)
            pubsub.subscribe(name)

        elif choice == 4:
            global exited
            exited = True
            receiver.join()
            exit(0)

        else:
            print("Error: invalid input")


if __name__ == '__main__':
    main()
