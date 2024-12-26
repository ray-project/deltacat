# Defines a glob path, allowing for files in the path to include variables
#       This is useful if external data contains keys within file path
class GlobPath:

    def __init__(self, path):
        """
        GlobPath allows for a mix of literal path components and variable placeholders.
        Variable placeholders are denoted by ${variable_name}

        Different URI formats will be allowed in the long term.
        RIGHT NOW - only unix glob format is accepted (https://docs.python.org/3/library/glob.html)

        Example:
        /path/to/${year}/${month}/data.csv
        ../foo/${bar}/**/*

        Variables elements MUST be between two slashes. E.g. /foo/bar/{baz}.csv is not allowed

        :param path: A string representing a glob path with variable placeholders.
        """
        self.path = path
        self.vars = self.parse_path()

    def parse_path(self):
        # Split the path into elements.
        vars = []
        path_parts = self.path.split('/')
        for part in path_parts:
            if part.startswith('${') and part.endswith('}'):
                # Variable element (e.g., ${date})
                # Append just the column name (e.g. "date")
                vars.append(part[2:-1])
        return vars

    def __str__(self):
        return f"GlobPath: {self.path}\nElements: {self.elements}"
