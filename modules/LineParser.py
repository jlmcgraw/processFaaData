from modules.FileDefinitions import definitions


class LineParser:
    def __init__(self, base_file_name: str, record_type: str, line_text: str) -> None:
        self.line_length = 0
        self.definition = definitions[base_file_name][record_type]
        self.line_dict = {}

        last_end = 0

        for key, value in self.definition.items():
            self.line_length += value
            next_end = last_end + value
            self.line_dict[key] = line_text[last_end:next_end].strip()
            last_end = next_end
