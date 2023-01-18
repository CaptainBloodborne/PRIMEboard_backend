import shlex


class WorkPiece:
    def __init__(self, workpiece: str):
        columns = workpiece.split("|")
        self._search_query = columns[0]
        self._preset_id = int(columns[1])
        self._active = columns[2]
        self._kind = columns[3]
        self._parent = columns[4]
        self._miner = columns[5]
        self._miner_args = columns[6]
        self._shard_kind = columns[7]
        self._query = columns[8]
        self._category = columns[9]

    def parse_miner_args(self):
        args_field = shlex.split(self._miner_args)

        json_body = dict()

        for arg in args_field:
            key, value = arg.split("=")

            if value.isdigit():
                value = int(value)

            if value == "true" or value == "True":
                value = True
            elif value == "false" or value == "False":
                value = False
            elif value == "-1":
                value = -1

            json_body[cmd_arg_name_merger(key)] = value

        return json_body

    @property
    def get_search_query(self):
        return self._search_query

    @property
    def get_preset_id(self):
        return self._preset_id

    @property
    def get_miner(self):
        return self._miner

    @property
    def get_query(self):
        return self._query

    @property
    def get_shard_kind(self):
        return self._shard_kind


def cmd_arg_name_merger(arg: str):
    name = ""

    if "context-subject-filter" in arg:
        return "contextFilter"

    if "--context-brand-filter" in arg:
        return "brandFilter"

    word_list = arg.strip("--").split("-")
    for word in word_list:

        if word_list.index(word) == 0:
            name += word
        else:
            name += word.capitalize()

    return name
