import json

def parse_parentheses(course, p_string, id):
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
        if '(' in val and ')' in val:
            id += 1
            inner_parsed, inner_values = parse_parentheses(course, val, id)
            new_c = {"id": id, "parsed": inner_parsed, "values": inner_values}
            values[values.index(val)] = new_c
    if len(stack) > 0:
        print(f"{course} - Unbalanced parentheses: Extra '('")
    return parsed, values


def main():
    stuff = {}
    with open("./data/message.txt", "r") as file:
        for line in file:
            content = line.split(": ")
            current_course = content[0]
            prereq = content[1].strip()
            if prereq == "":
                continue
            parsed, values = parse_parentheses(current_course, prereq, 0)
            json_structure = {"id": 0, "parsed": parsed, "values": values}
            stuff[current_course] = json_structure

    with open("./data/output.json", "w") as output:
        json.dump(stuff, output, indent=2)


if __name__ == "__main__":
    main()
