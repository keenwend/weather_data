import csv


def load_list_dicts_to_csv(path_to_new_csv, list_of_dicts):
    with open(path_to_new_csv, "w", newline="") as csv_file:
        fieldnames = list(list_of_dicts[0].keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter="|")
        writer.writeheader()
        writer.writerows(list_of_dicts)



