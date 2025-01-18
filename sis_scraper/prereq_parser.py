import json
from typing import List
import re

OVERRIDE_REGEX = r"( and | or ) *Prerequisite Override [0-9]*$"


class PrereqLevel(dict):
    current_id = 0

    def __init__(self, parsed: str, values):
        self.id = PrereqLevel.current_id
        PrereqLevel.current_id += 1
        self.values = []

        if parsed.find(" or ") > -1 and parsed.find(" and ") > -1:
            print(f"Error: {parsed}")
            self.type = "CONFLICT"
        else:
            self.type = "and" if parsed.find(" and ") > -1 else "or"
            self.values = [
                val.strip()
                for val in parsed.split(" " + self.type + " ")
                if val.strip() != "()"
            ]
        
        for val in values:
            if isinstance(val, PrereqLevel):
                self.values.append(val)
            elif val.find(" or ") > -1 or val.find(" and ") > -1:
                self.values.append(PrereqLevel(val, []))

    def toJSON(self):
        return {
            "id": self.id,
            "type": self.type,
            "values": [
                val.toJSON() if isinstance(val, PrereqLevel) else val
                for val in self.values
            ],
        }


def parse_parentheses(course, p_string):
    parsed = ""
    stack = []
    current = ""
    values = []
    for char in p_string:
        if char == "(":
            if len(stack) > 0:
                current += char
            else:
                if current != "":
                    values.append(current)
                parsed += char
            stack.append("(")
        elif char == ")":
            if len(stack) > 1:
                current += char
            elif len(stack) == 1:
                if current != "":
                    values.append(current)
                current = ""
                parsed += char
            else:
                print(f"{course} - Unbalanced parentheses: Early ')'")
            if len(stack) > 0:
                stack.pop()
        elif len(stack) > 0:
            current += char
        else:
            parsed += char

    if current != "":
        values.append(current)

    for val in values:
        if "(" in val and ")" in val:
            inner_parsed, inner_values = parse_parentheses(course, val)
            new_c = PrereqLevel(inner_parsed, inner_values)
            values[values.index(val)] = new_c
    if len(stack) > 0:
        print(f"{course} - Unbalanced parentheses: Extra '('")
    return parsed, values


def remove_prereq_override(string):
    while re.search(OVERRIDE_REGEX, string):
        string = re.sub(OVERRIDE_REGEX, "", string).strip()
    return string


def parse_prereq(course, string):
    if string == "":
        return {}
    string = remove_prereq_override(string)
    parsed, values = parse_parentheses(course, string)
    level = PrereqLevel(parsed, values).toJSON()
    PrereqLevel.current_id = 0
    return level


def main():
    stuff = {}
    with open("./data/message.txt", "r") as file:
        for line in file:
            content = line.split(": ")
            current_course = content[0]
            prereq = content[1].strip()
            json_structure = parse_prereq(current_course, prereq)
            stuff[current_course] = json_structure

    with open("./data/output.json", "w") as output:
        json.dump(stuff, output, indent=2)


if __name__ == "__main__":
    main()
