"""Seed loader framing, not a replacement for real performance acceptance."""

import pytest

from tests.integration_native_rebase_benchmark import seed_statements


def test_pinned_dump_keeps_quoted_semicolon_inside_single_insert(tmp_path):
    path = tmp_path / "seed.sql"
    path.write_text("CREATE TABLE t(\n id int, v varchar(20)\n);\n"
                    "INSERT INTO t VALUES(1,'semi;colon');\n")
    statements = list(seed_statements(path))
    assert len(statements) == 2
    assert statements[1] == "INSERT INTO t VALUES(1,'semi;colon');\n"


@pytest.mark.parametrize("content", ["CREATE TABLE t(id int);\n", "INSERT INTO t VALUES\n(1);\n",
                                     "INSERT INTO t VALUES(1);\nDROP TABLE t;\n"])
def test_unrecognized_dump_format_is_rejected(tmp_path, content):
    path = tmp_path / "seed.sql"
    path.write_text(content)
    with pytest.raises(ValueError):
        list(seed_statements(path))
