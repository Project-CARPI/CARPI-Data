import re

from app import logger


class PrereqLevel:
    """
    A class that represents a level of a prerequisite tree.

    Contains a type (and/or) and a list of values. Each of the values can either be a
    string (to represent a course), or another PrereqLevel (to represent a sublevel of
    the tree).
    """

    def __init__(self, parsed: str, values: list):
        """
        The constructor for the PrereqLevel class.

        Takes two arguments, a string with internal parentheses parsed out, and a list of
        those values from the parentheses.
        """
        self.id = 0
        self.values = []

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

    def set_id(self, id: int):
        """
        Sets the ID.

        This will be used in the database to keep track of the recursive tree structure.
        """
        self.id = id

    def get_levels(self):
        """Gets all the nested sublevels of the tree."""
        return [val for val in self.values if isinstance(val, PrereqLevel)]

    def to_json(self):
        """
        Converts the PrereqLevel to JSON, which is more easily used outside of this file.
        """
        return {
            "id": self.id,
            "type": self.type,
            "values": [
                val.to_json() if isinstance(val, PrereqLevel) else val
                for val in self.values
            ],
        }


class ParenthesisBalanceError(Exception):
    """
    A simple exception to indicate unbalanced parentheses in the prerequisite string.
    """

    pass


def parse_parentheses(p_string: str) -> tuple[str, list[str | PrereqLevel]]:
    """
    Given a prerequisite string, values inside of parentheses are recursively parsed. The
    initial string is returned with the parentheses emptied, and a list of the values
    from inside of those parentheses.
    """
    parsed = ""
    stack = []
    current = ""
    values = []
    # Using a stack, parse the string and extract values from inside parentheses
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

    # Then take the values from inside the parentheses and parse them again
    for val in values:
        if "(" in val and ")" in val:
            inner_parsed, inner_values = parse_parentheses(val)
            new_c = PrereqLevel(inner_parsed, inner_values)
            values[values.index(val)] = new_c
    if len(stack) > 0:
        raise ParenthesisBalanceError(f"Unbalanced parentheses: Extra '('")
    return parsed, values


def add_level_ids(level: PrereqLevel) -> None:
    """
    Adds IDs to levels in the tree, incrementing in a breadth-first order.
    """
    level.set_id(0)
    id = 1
    levels = level.get_levels()
    while len(levels) > 0:
        current: PrereqLevel = levels.pop(0)
        current.set_id(id)
        id += 1
        levels.extend(current.get_levels())


def trim_codes(level: PrereqLevel) -> None:
    """
    Recursively traverses the tree and trims values to only include department and code.
    """
    for i in range(len(level.values)):
        if isinstance(level.values[i], PrereqLevel):
            trim_codes(level.values[i])
        else:
            level.values[i] = trim_code(level.values[i])


def trim_code(code: str) -> str:
    """
    Trims a course code to only include the department and code, removing "Minimum Grade
    of" and "level" if present.
    """
    if code.find("Minimum Grade of") > -1:
        code = code.split("Minimum Grade of")[0].strip()
    if code.find(" level ") > -1:
        code = code.split(" level ")[1].strip()
    return code


def check_same_type(level: PrereqLevel) -> bool:
    """
    Checks if the level has a sublevel of the same type.
    """
    for val in level.values:
        if isinstance(val, PrereqLevel):
            if val.type == level.type:
                return True
            if check_same_type(val):
                return True
    return False


def remove_same_level(level: PrereqLevel) -> None:
    """
    If a sublevel has the same type as the parent, the sublevel is combined with the
    parent level.
    """
    for val in level.values:
        if isinstance(val, PrereqLevel):
            remove_same_level(val)
            if val.type == level.type:
                level.values.remove(val)
                level.values.extend(val.values)


def remove_prereq_overrides(level: PrereqLevel) -> None:
    """
    Recursively traverses the tree and removes any "Prerequisite Override" values.
    """
    i = 0
    while i < len(level.values):
        if isinstance(level.values[i], PrereqLevel):
            remove_prereq_overrides(level.values[i])
        elif level.values[i].find("Prerequisite Override") > -1:
            level.values.pop(i)
            continue
        i = i + 1


def collapse_single_course_levels(level: PrereqLevel) -> bool:
    """
    Recursively traverses the tree and converts any levels with a single course to just
    the course as the value within the PrereqLevel.
    """
    i = 0
    for i in range(len(level.values)):
        if isinstance(level.values[i], PrereqLevel):
            if collapse_single_course_levels(level.values[i]):
                level.values[i] = level.values[i].values[0]
    return len(level.values) == 1


def collapse_grandparent(level: PrereqLevel) -> PrereqLevel:
    """
    If a level has a single child, it is collapsed into its parent.

    Meant to be used after collapse_single_course_levels.
    """
    if len(level.values) == 1 and isinstance(level.values[0], PrereqLevel):
        return level.values[0]
    return level


def set_default_type(level: PrereqLevel) -> None:
    """
    Recursively traverses the tree and sets the type of the level to "or" if there is only
    one value.
    """
    i = 0
    while i < len(level.values):
        if isinstance(level.values[i], PrereqLevel):
            set_default_type(level.values[i])
        i = i + 1
    if len(level.values) <= 1:
        level.type = "or"


def remove_empty_levels(level: PrereqLevel) -> bool:
    """
    Recursively traverses the tree and removes levels without any values.
    """
    i = 0
    while i < len(level.values):
        if isinstance(level.values[i], PrereqLevel):
            if remove_empty_levels(level.values[i]):
                level.values.pop(i)
                continue
        i = i + 1
    return len(level.values) == 0


def fix_wildcards(level: PrereqLevel) -> None:
    """
    Recursively traverses the tree and replaces any anything other than numbers in course
    codes with "x".
    """
    for i in range(len(level.values)):
        if isinstance(level.values[i], PrereqLevel):
            fix_wildcards(level.values[i])
        else:
            level.values[i] = fix_wildcard(level.values[i])


def fix_wildcard(code: str) -> str:
    """
    Given a course code, replaces any non-numeric characters with "x".
    """
    dept = code[:4]
    num = code[5:]
    new_num = ""
    CODE_LENGTH = 4
    WILDCARD = "x"
    for i in range(len(num)):
        if num[i].isdigit():
            new_num += num[i]
        else:
            new_num += WILDCARD
    new_num = new_num.ljust(CODE_LENGTH, WILDCARD)[:CODE_LENGTH]
    return dept + " " + new_num


def check_values(course: str, level: PrereqLevel) -> None:
    """
    Recursively checks the values in the tree to ensure they are in the correct format.
    """
    VALUE_CHECK_REGEX = r"^[A-Z]{4} ([0-9]|x){4}$"
    for val in level.values:
        if isinstance(val, PrereqLevel):
            check_values(course, val)
        elif re.match(VALUE_CHECK_REGEX, val) is None:
            raise Exception("Error parsing prereqs for " + course + " - " + val)


def parse_prereq(term: str, crn: str, prereq_string: str) -> dict:
    """
    Given a term, CRN, and prerequisite string, parses the string into a JSON object that
    represents a tree structure of the prerequisites.

    The term and CRN are used only in error messages to assist with debugging if any data
    is invalid.

    Examples:

    `"CSCI 1100"`
    ```
    {
        "id": 0,
        "type": "or",
        "values": [
            "CSCI 1100"
        ]
    }
    ```

    `"CSCI 1100 and MATH 1010"`
    ```
    {
        "id": 0,
        "type": "and",
        "values": [
            "CSCI 1100"
            "MATH 1010"
        ]
    }
    ```

    `"((CSCI 1100 and MATH 1010) or CSCI 1200)"`
    ```
    {
        "id": 0,
        "type": "or",
        "values": [
            {
                "id": 1,
                "type": "and",
                "values": [
                    "CSCI 1100",
                    "MATH 1010"
                ]
            },
            "CSCI 1200"
        ]
    }
    """
    if prereq_string == "":
        return {}
    try:
        parsed, values = parse_parentheses(prereq_string)
    except ParenthesisBalanceError as e:
        logger.error(f"Error parsing prerequisites for CRN {crn} in term {term} - {e}")
        return {}
    level = PrereqLevel(parsed, values)
    remove_prereq_overrides(level)
    collapse_single_course_levels(level)
    level = collapse_grandparent(level)
    if remove_empty_levels(level):
        return {}
    set_default_type(level)
    while check_same_type(level):
        remove_same_level(level)
    add_level_ids(level)
    trim_codes(level)
    fix_wildcards(level)
    # check_values(course, level)
    return level.to_json()
