import json
import logging
from pathlib import Path
from typing import Any


def codify_course_code(course_code: str, subject_code_name_map: dict[str, str]) -> str:
    course_code_list = course_code.split(" ")
    if len(course_code_list) != 2:
        logging.warning(f"Unexpected course code format: {course_code}")
        return course_code

    subject_name = course_code_list[0]
    course_number = course_code_list[1]
    # Translate subject_name (full name) back to its code using subject_code_name_map
    # subject_code_name_map: {code: name}
    # We need to find the code whose value matches subject_name
    code = next(
        (k for k, v in subject_code_name_map.items() if v == subject_name), subject_name
    )
    return f"{code} {course_number}"


def codify_attribute(attribute: str) -> str:
    attribute_list = attribute.split("  ")
    if len(attribute_list) != 2:
        logging.warning(f"Unexpected attribute format: {attribute}")
        return attribute
    return attribute_list[1]


def codify_restriction(
    restriction: str, restriction_code_name_map: dict[str, str]
) -> str:
    pass


def post_process(
    term_course_data: dict[str, Any],
    attribute_code_name_map: dict[str, str],
    restriction_code_name_map: dict[str, str],
    subject_code_name_map: dict[str, str],
    instructor_rcsid_name_map: dict[str, str],
) -> None:
    for subject_code, subject_data in term_course_data.items():
        subject_courses = subject_data["courses"]
        for course_code, course_data in subject_courses.items():
            course_detail = course_data["course_detail"]
            course_corequisites = course_detail["corequisite"]
            course_prerequisites = course_detail["prerequisite"]
            course_crosslists = course_detail["crosslist"]
            course_attributes = course_detail["attributes"]
            course_restriction_types = course_detail["restriction_types"]
            course_sections = course_detail["sections"]
            for i, corequisite in enumerate(course_corequisites):
                course_corequisites[i] = codify_course_code(
                    corequisite, subject_code_name_map
                )
            for i, prerequisite in enumerate(course_prerequisites):
                pass
            for i, crosslist in enumerate(course_crosslists):
                pass
            for i, attribute in enumerate(course_attributes):
                course_attributes[i] = codify_attribute(attribute)
            for restriction_type in course_restriction_types:
                restriction_type = restriction_type.replace("not_", "")
                pass
            for section in course_sections:
                instructor_list = section["instructor"]
                pass


def main(
    output_data_dir: Path | str,
    processed_output_data_dir: Path | str,
    attribute_code_name_map_path: Path | str,
    instructor_rcsid_name_map_path: Path | str,
    restriction_code_name_map_path: Path | str,
    subject_code_name_map_path: Path | str,
) -> bool:
    # Validate input directories
    if not all(
        (
            output_data_dir,
            processed_output_data_dir,
            attribute_code_name_map_path,
            instructor_rcsid_name_map_path,
            restriction_code_name_map_path,
            subject_code_name_map_path,
        )
    ):
        logging.error("One or more required directories are not specified.")
        return False

    # Convert to Path objects if necessary
    if isinstance(output_data_dir, str):
        output_data_dir = Path(output_data_dir)
    if isinstance(processed_output_data_dir, str):
        processed_output_data_dir = Path(processed_output_data_dir)
    if isinstance(attribute_code_name_map_path, str):
        attribute_code_name_map_path = Path(attribute_code_name_map_path)
    if isinstance(instructor_rcsid_name_map_path, str):
        instructor_rcsid_name_map_path = Path(instructor_rcsid_name_map_path)
    if isinstance(restriction_code_name_map_path, str):
        restriction_code_name_map_path = Path(restriction_code_name_map_path)
    if isinstance(subject_code_name_map_path, str):
        subject_code_name_map_path = Path(subject_code_name_map_path)

    # Validate input directories
    if not output_data_dir.exists() or not output_data_dir.is_dir():
        logging.error(f"Output data directory {output_data_dir} does not exist.")
        return False

    # Validate mapping files
    for map_path in [
        attribute_code_name_map_path,
        instructor_rcsid_name_map_path,
        restriction_code_name_map_path,
        subject_code_name_map_path,
    ]:
        if not map_path.exists() or map_path.is_dir():
            logging.error(f"Mapping file {map_path} does not exist or is a directory.")
            return False

    # Load code mappings
    with attribute_code_name_map_path.open("r", encoding="utf-8") as f:
        attribute_code_name_map = json.load(f)
    with instructor_rcsid_name_map_path.open("r", encoding="utf-8") as f:
        instructor_rcsid_name_map = json.load(f)
    with restriction_code_name_map_path.open("r", encoding="utf-8") as f:
        restriction_code_name_map = json.load(f)
    with subject_code_name_map_path.open("r", encoding="utf-8") as f:
        subject_code_name_map = json.load(f)

    # Process each term course data file
    for term_file in output_data_dir.glob("*.json"):
        logging.info(f"Processing term course data file: {term_file}")
        with term_file.open("r", encoding="utf-8") as f:
            term_course_data = json.load(f)

        post_process(
            term_course_data,
            attribute_code_name_map,
            restriction_code_name_map,
            subject_code_name_map,
            instructor_rcsid_name_map,
        )

        # Write processed data
        processed_file_path = processed_output_data_dir / term_file.name
        with processed_file_path.open("w", encoding="utf-8") as f:
            json.dump(term_course_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Wrote processed data to {processed_file_path}")

    return True


if __name__ == "__main__":
    pass
