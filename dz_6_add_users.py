import pandas as pd
import socket
import struct
from tqdm import tqdm
from faker import Faker
from random import randint, choice
from pathlib import Path

file_path = Path(__file__).parent
fake = Faker()
users = pd.DataFrame(columns='user_id user_name ip_address dns_name'.split())
users_log = pd.DataFrame(columns='user_id web_site bytes'.split())
sites = [fake.bothify(text=f"www.{'?' * randint(3, 77)}.{'?' * randint(2, 3)}").lower()
         for _ in range(777)]

for i in range(10):
    mask = f"www.{'?' * randint(5, 7)}.{'?' * 2}"
    name = fake.bothify(text='?' * randint(8, 25))
    ip = socket.inet_ntoa(struct.pack('>I', randint(1, 0xffffffff)))
    dns = fake.bothify(text=mask).lower()
    users.loc[len(users)] = [i + 1, name, ip, dns]
    print('\n', i + 1, name, ip, dns, '\n')

    for _ in tqdm(range(10_000)):
        users_log.loc[len(users_log)] = [i + 1, choice(sites), randint(0, 10 ** 9)]

users.to_csv(file_path.joinpath('users.csv'), index=False)
users_log.to_csv(file_path.joinpath('users_log.csv'), index=False)
print(users.head(10))
print(users_log.bytes.min(), users_log.bytes.max())
