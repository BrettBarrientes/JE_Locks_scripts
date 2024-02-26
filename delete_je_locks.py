import redis
import data


def delete_multiple_keys(selected_host, redis_port, keys_to_delete):
    try:
        # Connect to the Redis server
        redis_client = redis.StrictRedis(host=selected_host, port=redis_port, decode_responses=True)

        # Delete the specified keys
        deleted_keys = redis_client.delete(*keys_to_delete)

        return deleted_keys
    except redis.exceptions.ConnectionError:
        print("Error: Failed to connect to the Redis server. Make sure the VPN is connected.")
        return None

if __name__ == "__main__":
    # Replace these with your Redis server configuration
    redis_hosts = {1: data.host1, 
                   2: data.host2, 
                   3: data.host3,
                   4: data.host4}
    redis_port = 6379

    selected_host_envrionment = "Select the environment to delete the keys: \n1. QA10 \n2. UAT \n3. INTTEST \n4. PROD\n "
    print(selected_host_envrionment)

    selected_host_envrionment_number = input("Enter a number to select the environment to delete the keys: ")
    # check if the input is valid and 
    if selected_host_envrionment_number.isdigit() in redis_hosts:
        selected_host = redis_hosts[int(selected_host_envrionment_number)]
        if selected_host_envrionment_number == "1":
            print("QA10 environment has been selected")
        elif selected_host_envrionment_number == "2":
            print("UAT environment has been selected")
        elif selected_host_envrionment_number == "3":
            print("INTTEST environment has been selected")
        elif selected_host_envrionment_number == "4":
            print("PROD environment has been selected")
    else:
        print("Invalid input. Please try again.")
        exit()        
        

    r = redis.Redis(host=selected_host, port=redis_port, decode_responses=True)
    # Insert the Client ID
    client_id = input("Enter the Client ID: ")
    # List of keys you want to delete
    keys_to_delete = list(r.scan_iter("Edit_"+client_id+"_*"))

    # Call the function to delete the keys
    print(keys_to_delete)

    # print a message to the user to confirm the keys to be deleted #

    deleted_keys_count = delete_multiple_keys(selected_host, redis_port, keys_to_delete)

    if deleted_keys_count is not None:
        print(f"Successfully deleted {deleted_keys_count} keys.")
    else:
        print("Keys deletion failed.")