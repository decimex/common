from decimex.IssuesDb.IssuesDb import IssuesDb

def test_sanity():
    db_type     = "postgres"
    user_name   = "finder"
    password    = "Aa123456"
    host        = "localhost"
    port        = "5432"
    db_name     = "issues"

    db = IssuesDb(db_type, user_name, password, host, port, db_name)
    db.initialize()

    with db.get_session() as session:
        db.create_defined_tables()
        db.add_link(session, "blabla")
        db.add_link(session, "gaga")
        db.get_link(session, url="gaga")
        db.get_first_link(session)
        db.update_link_is_parsed(session, url="gaga", is_parsed=False)
        db.delete_link(session, is_parsed=True)
        db.commit_session(session)
        results = db.get_link(session, url="gaga")
        for result in results:
            assert result.is_parsed == False
