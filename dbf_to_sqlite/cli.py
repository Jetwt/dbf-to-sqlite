import click
import dbf
from pathlib import Path
from sqlite_utils import Database


@click.command()
@click.version_option()
@click.argument("dbf_paths", type=click.Path(exists=True), nargs=-1, required=True)
@click.argument("sqlite_db", nargs=1)
@click.option("-cp","--codepage", default='cp936', help="Set codepage for dbf files (default: cp936)")
@click.option("-v", "--verbose", help="Show what's going on", is_flag=True)
@click.option("--table", help="Table name to use (only valid for single files)")
def cli(dbf_paths, sqlite_db, table, verbose, codepage):
    """
    Convert DBF files (dBase, FoxPro etc) to SQLite

    https://github.com/simonw/dbf-to-sqlite
    """
    if table and len(dbf_paths) > 1:
        raise click.ClickException("--table only works with a single DBF file")
    db = Database(sqlite_db)
    for path in dbf_paths:
        table_name = table if table else Path(path).stem
        if verbose:
            click.echo('Loading {} into table "{}"'.format(path, table_name))
        # table = dbf.Table(str(path))
        dbfTable = dbf.Table(str(path), codepage=codepage)
        dbfTable.open()
        columns = dbfTable.field_names
        db[table_name].insert_all({col: value.strip() if isinstance(value, str) else value for col, value in zip(columns, list(row))} for row in dbfTable)
        # db[table_name].insert_all(dict(zip(columns, list(row))) for row in dbfTable)
        dbfTable.close()
    db.vacuum()


if __name__ == "__main__":
    cli()
