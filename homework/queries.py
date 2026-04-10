"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

import os
import csv
from collections import defaultdict


def execute_query_1():
    """Query 1: Average tip by day"""
    results = defaultdict(list)
    
    with open("files/input/tips.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            day = row["day"]
            tip = float(row["tip"])
            results[day].append(tip)
    
    output_data = []
    for day, tips in sorted(results.items()):
        avg_tip = sum(tips) / len(tips)
        output_data.append(f"{day},{avg_tip:.2f}\n")
    
    return output_data


def execute_query_2():
    """Query 2: Average tip by sex"""
    results = defaultdict(list)
    
    with open("files/input/tips.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sex = row["sex"]
            tip = float(row["tip"])
            results[sex].append(tip)
    
    output_data = []
    for sex, tips in sorted(results.items()):
        avg_tip = sum(tips) / len(tips)
        output_data.append(f"{sex},{avg_tip:.2f}\n")
    
    return output_data


def execute_query_3():
    """Query 3: Total bill by day"""
    results = defaultdict(list)
    
    with open("files/input/tips.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            day = row["day"]
            total_bill = float(row["total_bill"])
            results[day].append(total_bill)
    
    output_data = []
    for day, bills in sorted(results.items()):
        total = sum(bills)
        output_data.append(f"{day},{total:.2f}\n")
    
    return output_data


def execute_query_4():
    """Query 4: Average tip by time"""
    results = defaultdict(list)
    
    with open("files/input/tips.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            time = row["time"]
            tip = float(row["tip"])
            results[time].append(tip)
    
    output_data = []
    for time, tips in sorted(results.items()):
        avg_tip = sum(tips) / len(tips)
        output_data.append(f"{time},{avg_tip:.2f}\n")
    
    return output_data


def execute_query_5():
    """Query 5: Average tip by party size"""
    results = defaultdict(list)
    
    with open("files/input/tips.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            size = row["size"]
            tip = float(row["tip"])
            results[size].append(tip)
    
    output_data = []
    for size, tips in sorted(results.items(), key=lambda x: int(x[0])):
        avg_tip = sum(tips) / len(tips)
        output_data.append(f"{size},{avg_tip:.2f}\n")
    
    return output_data


def write_query_output(query_num, data):
    """Write query output in MapReduce format"""
    output_dir = f"files/query_{query_num}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Write part-00000 file
    with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
        f.writelines(data)
    
    # Write _SUCCESS file
    with open(f"{output_dir}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")


#
# ORQUESTADOR:
#
def run():
    """Orquestador"""
    queries = [
        execute_query_1,
        execute_query_2,
        execute_query_3,
        execute_query_4,
        execute_query_5,
    ]
    
    for query_num, query_func in enumerate(queries, 1):
        data = query_func()
        write_query_output(query_num, data)


if __name__ == "__main__":

    run()