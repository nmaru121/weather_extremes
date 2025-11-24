import json

def main():
    with open("data/updates.json", "r") as f:
        updates = json.load(f)
    updates = updates [::-1] # Reverse to have oldest first
    title = input("Enter update title: ")
    description = input("Enter update description: ")
    updates.append({"title": title, "description": description})
    updates = updates [::-1] # Reverse back to have newest first
    with open("data/updates.json", "w") as f:
        json.dump(updates, f)
    print("Update added successfully.")
    return

if __name__ == "__main__":
    main()