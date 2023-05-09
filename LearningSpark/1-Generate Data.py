import sys
import random
import csv
import os

def get_random_choice(lst):
    return random.choice(lst)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: gen_mnm_dataset entries", file=sys.stderr)
        sys.exit(-1)

    states = ["CA", "WA", "TX", "NV", "CO", "OR", "AZ", "WY", "NM", "UT"]
    colors = ["Brown", "Blue", "Orange", "Yellow", "Green", "Red"]
    fieldnames = ['State', 'Color', 'Count']

    entries = int(sys.argv[1])
    dataset_fn = "1-mnm_dataset.csv"
    dataset_folder = "data"
    dataset_path = os.path.join(dataset_folder, dataset_fn)

    if not os.path.exists(dataset_folder):
        os.makedirs(dataset_folder)

    with open(dataset_path, mode='w', newline='') as dataset_file:
        writer = csv.DictWriter(dataset_file, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(entries):
            writer.writerow({'State': get_random_choice(states), 'Color': get_random_choice(colors), 'Count': random.randint(10, 100)})

    print(f"Wrote {entries} lines in {dataset_fn} file in {dataset_folder} folder")
