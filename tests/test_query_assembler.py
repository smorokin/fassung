from fassung.query_assembler import QueryAssembler


async def test_query_assembler_no_placeholders() -> None:
    query_assembler = QueryAssembler()
    query = t"SELECT * FROM table1"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query == "SELECT * FROM table1"
    assert assembled.args == ()


async def test_query_assembler_2_placeholders_same_value() -> None:
    query_assembler = QueryAssembler()
    var = 1
    query = t"SELECT * FROM table1 WHERE id = {var} AND field1 = {var}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query == "SELECT * FROM table1 WHERE id = $1 AND field1 = $2"
    assert assembled.args == (1, 1)


async def test_query_assembler_2_placeholders_different_values() -> None:
    query_assembler = QueryAssembler()
    var1 = 1
    var2 = 2
    query = t"SELECT * FROM table1 WHERE id = {var1} AND field1 = {var2}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query == "SELECT * FROM table1 WHERE id = $1 AND field1 = $2"
    assert assembled.args == (1, 2)


async def test_query_assembler_3_placeholders() -> None:
    query_assembler = QueryAssembler()
    var1 = 1
    var2 = 2
    var3 = 3
    query = t"SELECT * FROM table1 WHERE id = {var1} AND field1 = {var2} AND field2 = {var3}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query == "SELECT * FROM table1 WHERE id = $1 AND field1 = $2 AND field2 = $3"
    assert assembled.args == (1, 2, 3)


async def test_query_assembler_simple_nested_query_without_placeholders() -> None:
    query_assembler = QueryAssembler()
    var = 1
    sub_query = t"COUNT(*)"
    query = t"SELECT {sub_query} FROM table1 WHERE field1 = {var}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query.replace("  ", " ") == "SELECT COUNT(*) FROM table1 WHERE field1 = $1"
    assert assembled.args == (1,)


async def test_query_assembler_simple_nested_query_with_different_placeholders() -> None:
    query_assembler = QueryAssembler()
    var1 = 1
    var2 = 2
    sub_query = t"AND field2 = {var2}"
    query = t"SELECT * FROM table1 WHERE field1 = {var1} {sub_query}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query.replace("  ", " ") == "SELECT * FROM table1 WHERE field1 = $1 AND field2 = $2"
    assert assembled.args == (1, 2)


async def test_query_assembler_multiple_nested_queries_without_placeholders() -> None:
    query_assembler = QueryAssembler()
    var = 1
    sub_sub_query = t"AND field3 = 3"
    sub_query = t"AND field2 = 2 {sub_sub_query}"
    query = t"SELECT * FROM table1 WHERE field1 = {var} {sub_query}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert assembled.query.replace("  ", " ") == "SELECT * FROM table1 WHERE field1 = $1 AND field2 = 2 AND field3 = 3"
    assert assembled.args == (1,)


async def test_query_assembler_multiple_nested_queries_with_different_placeholders() -> None:
    query_assembler = QueryAssembler()
    var1 = 1
    var2 = 2
    var3 = 3
    sub_sub_query = t"AND field3 = {var3}"
    sub_query = t"AND field2 = {var2} {sub_sub_query}"
    query = t"SELECT * FROM table1 WHERE field1 = {var1} {sub_query}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert (
        assembled.query.replace("  ", " ") == "SELECT * FROM table1 WHERE field1 = $1 AND field2 = $2 AND field3 = $3"
    )
    assert assembled.args == (1, 2, 3)


async def test_query_assembler_multiple_nested_queries_with_same_placeholder() -> None:
    query_assembler = QueryAssembler()
    var = 1
    sub_sub_query = t"AND field3 = {var}"
    sub_query = t"AND field2 = {var} {sub_sub_query}"
    query = t"SELECT * FROM table1 WHERE field1 = {var} {sub_query}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert (
        assembled.query.replace("  ", " ") == "SELECT * FROM table1 WHERE field1 = $1 AND field2 = $2 AND field3 = $3"
    )
    assert assembled.args == (1, 1, 1)


async def test_query_assembler() -> None:
    query_assembler = QueryAssembler()
    sub_sub_sub_query = t"field3"
    sub_sub_query = t"field2, {sub_sub_sub_query}"
    sub_query = t"id, field1, {sub_sub_query}"
    var = 1

    other_sub_sub_query = t"AND field2 = {var}"
    other_sub_query = t"AND field1 = {var} {other_sub_sub_query}"

    query = t"SELECT {sub_query} FROM table1 WHERE id = {var} {other_sub_query}"
    assembled = query_assembler.assemble(query)
    assert isinstance(assembled.query, str)
    assert isinstance(assembled.args, tuple)
    assert (
        assembled.query.replace("\n", "").replace("  ", " ")
        == "SELECT id, field1, field2, field3 FROM table1 WHERE id = $1 AND field1 = $2 AND field2 = $3"
    )
    assert assembled.args == (1, 1, 1)
