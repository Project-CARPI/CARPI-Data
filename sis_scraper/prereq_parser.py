import json
import re


class PrereqLevel:
    def __init__(self, parsed: str, values):
        self.id = 0
        self.values = []

        parsed = remove_prereq_override(parsed)

        self.type = "and" if parsed.find(" and ") > -1 else "or"
        for val in parsed.split(" " + self.type + " "):
            if val.strip() != "()":
                if val.find(" or ") > -1 or val.find(" and ") > -1:
                    self.values.append(PrereqLevel(val, []))
                else:
                    self.values.append(val.strip())

        for val in values:
            if isinstance(val, PrereqLevel):
                self.values.append(val)
            elif val.find(" or ") > -1 or val.find(" and ") > -1:
                self.values.append(PrereqLevel(val, []))
            else:
                self.values.append(val)

    def set_id(self, id):
        self.id = id

    def get_levels(self):
        return [val for val in self.values if isinstance(val, PrereqLevel)]

    def to_json(self):
        return {
            "id": self.id,
            "type": self.type,
            "values": [
                val.to_json() if isinstance(val, PrereqLevel) else val
                for val in self.values
            ],
        }


class ParenthesisBalanceError(Exception):
    pass


def parse_parentheses(p_string):
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
                raise ParenthesisBalanceError(f"Unbalanced parentheses: Early ')'")
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
            inner_parsed, inner_values = parse_parentheses(val)
            new_c = PrereqLevel(inner_parsed, inner_values)
            values[values.index(val)] = new_c
    if len(stack) > 0:
        raise ParenthesisBalanceError(f"Unbalanced parentheses: Extra '('")
    return parsed, values


def remove_prereq_override(string):
    OVERRIDE_REGEX = r"( and | or ) *Prerequisite Override [0-9]*$"
    while re.search(OVERRIDE_REGEX, string):
        string = re.sub(OVERRIDE_REGEX, "", string).strip()
    return string


def add_level_ids(level: PrereqLevel):
    level.set_id(0)
    id = 1
    levels = level.get_levels()
    while len(levels) > 0:
        current: PrereqLevel = levels.pop(0)
        current.set_id(id)
        id += 1
        levels.extend(current.get_levels())


def trim_codes(level: PrereqLevel):
    for i in range(len(level.values)):
        if isinstance(level.values[i], PrereqLevel):
            trim_codes(level.values[i])
        else:
            level.values[i] = trim_code(level.values[i])


def trim_code(code: str):
    if code.find("Minimum Grade of") > -1:
        code = code.split("Minimum Grade of")[0].strip()
    if code.find(" level ") > -1:
        code = code.split(" level ")[1].strip()
    return code


def check_same_type(level: PrereqLevel):
    for val in level.values:
        if isinstance(val, PrereqLevel):
            if val.type == level.type:
                return True
            if check_same_type(val):
                return True
    return False


def remove_same_level(level: PrereqLevel):
    for val in level.values:
        if isinstance(val, PrereqLevel):
            if val.type == level.type:
                level.values.remove(val)
                level.values.extend(val.values)
            remove_same_level(val)


def parse_prereq(course, string):
    if string == "":
        return {}
    string = remove_prereq_override(string)
    try:
        parsed, values = parse_parentheses(string)
    except ParenthesisBalanceError as e:
        print(f"{course} - {e}")
        return {}
    level = PrereqLevel(parsed, values)
    while check_same_type(level):
        remove_same_level(level)
    add_level_ids(level)
    trim_codes(level)
    return level.to_json()


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
