from restaurantos.common.sqlfiles import schema_files


def test_shared_files_come_first_and_in_name_order():
    for kind in ("region", "hq"):
        files = schema_files(kind)
        folders = [f.parent.name for f in files]
        assert folders == sorted(folders, key=lambda d: d != "shared")
        assert folders[0] == "shared" and folders[-1] == kind
        for folder in set(folders):
            in_folder = [f.name for f in files if f.parent.name == folder]
            assert in_folder == sorted(in_folder)
