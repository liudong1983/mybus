import redis
if __name__ == '__main__':
    client =redis.Redis(host="10.73.11.21", port=10000)
    aa=client.execute_command("info")
    print aa
