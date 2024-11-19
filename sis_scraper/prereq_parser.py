import json


def parse_parentheses(s):
    parsed = ""
    stack = []
    current = ""
    contents = []
    for char in s:
        if char == "(":
            if len(stack) > 0:
                current += char
            else:
                if current != "":
                    contents.append(current)
                parsed += char
            stack.append("(")
        elif char == ")":
            if len(stack) > 1:
                current += char
            elif len(stack) == 1:
                if current != "":
                    contents.append(current)
                current = ""
                parsed += char
            else:
                parsed += char
            stack.pop()
        elif len(stack) > 0:
            current += char
        else:
            parsed += char
    if current != "":
        contents.append(current)
    return contents, parsed


def main():
    stuff = {}
    with open("./data/message.txt", "r") as file:
        for line in file:
            content = line.split(": ")
            current_course = content[0]
            prereq = content[1].strip()
            if prereq == "":
                continue
            stuff[current_course + " original"] = prereq
            values, parsed = parse_parentheses(prereq)

            json_structure = {"parsed": parsed, "values": values}
            stuff[current_course] = json_structure

    with open("./data/output.json", "w") as output:
        json.dump(stuff, output, indent=2)


if __name__ == "__main__":
    main()
