import re
import sqlite3


def comment(str):
    """

    :param str:
    :return: reuslt of searching a comment symbol in str
    """
    return re.match('%', str)


def parse(adr):
    """
    :param adr:
    :return: list of dictionaries which represent an example of bibtex obj
    """
    with open(adr, encoding='utf-8') as file:
        text = file.read()
    records = [i for i in re.split('\n@', text) if not comment(i)]
    search_data = re.compile("\s*(?P<name>\S*)\s*=\s?{\s?(?P<value>.*)\s?}"). findall
    search_description = re.compile("\s*@(?P<edition>.*){(?P<tag>.*),").findall

    result = []
    for r in records:
        data = search_data(r)
        if not r.startswith('@'):   # returning @, which was delete in splitting process
            r = '@' + r
        description = search_description(r)
        description = [('edition', description[0][0]), ('tag', description[0][1])]
        result.append(dict(description+data))   # creating a list of dictionaries which represent a book/article

    return result


def create_author_table():
    curs.execute(
        'create table authors('
        '   aut_id integer primary key autoincrement,'
        '   aut_name text'
        ')'
    )


def create_article_table():
    curs.execute(
        'create table articles('
        '   art_id integer primary key autoincrement,'
        '   title text,'
        '   aut_id integer,'
        '   journal text,'
        '   year text,'
        '   pages text,'
        '   volume text,'
        '   file text,'
        '   language text,'
        '   tag text,'
        '   timestamp text,'
        '   foreign key(aut_id) references authors(aut_id)'
        ')'
    )


def create_book_table():
    curs.execute(
        'create table books('
        '   book_id integer primary key autoincrement,'
        '   title text,'
        '   aut_id integer,'
        '   publisher text,'
        '   year integer,'
        '   numpages text,'
        '   language text,'
        '   address text,'
        '   file text,'
        '   tag text,'
        '   foreign key(aut_id) references authors(aut_id)'
        ')'
    )


def crete_conference_table():
    curs.execute(
        'create table conferences('
        '   conf_id integer primary key autoincrement,'
        '   title text,'
        '   aut_id integer,'
        '   booktitle text,'
        '   year text,'
        '   language text,'
        '   pages integer,'
        '   tag text,'
        '   foreign key(aut_id) references authors(aut_id)'
        ')'
    )

def create_thesises_table():
    curs.execute(
        'create table thesises('
        '   thesis_id integer primary key autoincrement,'
        '   title text,'
        '   aut_id integer,'
        '   school text,'
        '   year integer,'
        '   address text,'
        '   type text,'
        '   language text,'
        '   number text,'
        '   numpages integer,'
        '   foreign key(aut_id) references authors(aut_id)'
        ')'
    )

def create_booklets_table():
    curs.execute(
        'create table booklets('
        '   booklet_id integer primary key,'
        '   title text,'
        '   nite text,'
        '   tag text'
        ')'
    )


conn = sqlite3.connect('bib_lib.db')
curs = conn.cursor()

#create_author_table()
#create_article_table()
#create_book_table()
#crete_conference_table()
#create_thesises_table()
#create_booklets_table()

def fill_authors(data):
    """
    filling the authors table
    :param data:

    """
    authors = [d.get('Author') for d in data]
    for a in authors:
        curs.execute(r'select distinct aut_name from authors;')
        if (a,) not in curs.fetchall():
            curs.execute(r"insert into authors(aut_name) values(?)", [a])
        conn.commit()


def fill_articles(data):
    """
    filling the articles table
    :param data:
    """
    articles = [d for d in data if d.get('edition') == 'Article']
    for a in articles:
        curs.execute(r'select aut_id from authors where aut_name=?;', [a.get('Author')])
        aut_id = curs.fetchone()[0]
        curs.execute("insert into articles(title, aut_id, journal, year, pages, volume, file, language, tag, timestamp)"
                     "values(?,?,?,?,?,?,?,?,?,?);", (a.get('Title', 'Null'), int(aut_id), a.get('Journal', "Null"),
                                                      str(a.get('Year', "Null")), a.get('Pages', "Null"),
                                                      str(a.get('Volume', 'Null')), a.get('File', 'Null'),
                                                      a.get('Language', 'Null'), a.get('tag', 'Null'),
                                                      a.get('Timestamp', 'Null')
                                                      )
                     )
        conn.commit()


def fill_books(data):
    """
    filling the books table
    :param data:

    """
    books = [d for d in data if d.get('edition') == 'Book']
    for b in books:
        curs.execute(r'select aut_id from authors where aut_name=?;', [b.get('Author')])
        aut_id = curs.fetchone()[0]
        curs.execute(r"insert into books(title, aut_id, publisher, year, numpages, language, address, file, tag)"
                     r"values(?,?,?,?,?,?,?,?,?);", (b.get('Title', 'Null'), int(aut_id), str(b.get('Publisher', "Null")),
                                                     str(b.get('Year', "Null")), str(b.get('Numpages', "Null")),
                                                     b.get('Language', "Null"), b.get('Address', "Null"),
                                                     b.get('File', 'Null'), b.get("tag", "Null")
                                                     )
                     )
        conn.commit()


def fill_conference(data):  # filling the conferences table
    """
    filling the conferences table
    :param data:
    """
    conferences = [c for c in data if c.get('edition') == 'Conference']
    for c in conferences:
        curs.execute(r'select aut_id from authors where aut_name=?;', [c.get('Author')])
        aut_id = curs.fetchone()[0]
        curs.execute(r"insert into conferences(title, aut_id, booktitle, year, language, pages, tag)"
                     r"values(?,?,?,?,?,?,?);", (c.get('Title', 'Null'), int(aut_id), str(c.get('Booktitle', "Null")),
                                                 str(c.get('Year', "Null")), c.get('Language', "Null"),
                                                 c.get('Pages', 'Null'), c.get("tag", "Null")
                                                 )
                     )
        conn.commit()


def fill_thesises(data):
    """
    filling the thesises table
    :param data:
    """
    thesises = [t for t in data if t.get('edition') == 'PhdThesis']
    for t in thesises:
        curs.execute(r'select aut_id from authors where aut_name=?;', [t.get('Author')])
        aut_id = curs.fetchone()[0]
        curs.execute(r"insert into thesises(title, aut_id, school, year, address, type, "
                     r"language, number, numpages, tag)" r"values(?,?,?,?,?,?,?,?,?,?);",
                     (t.get('Title', 'Null'), int(aut_id), t.get('School', "Null"),
                      str(t.get('Year', "Null")), t.get('Address', 'Null'), t.get('Type', 'Null'),
                      t.get('Language', "Null"), t.get('Number', 'Null'), t.get('Numpages', 'Null'), t.get("tag", "Null")
                      )
                     )
        conn.commit()


def fill_booklets(data):
    """
    filling the booklets table
    :param data:
    """
    booklets = [b for b in data if b.get('edition') == 'Booklet']
    for b in booklets:
        curs.execute(r"insert into booklets(title, nite, tag)" r"values(?,?,?);",
                     (b.get('Title', 'Null'), b.get('Nite', "Null"), b.get("tag", "Null"))
                     )
        conn.commit()


def select_by_author(author):
    """

    :param author:
    :return: a list of object titles with this author
    """
    curs.execute(r'select aut_id from authors where aut_name=?;', [author])
    aut_id = curs.fetchone()[0]
    curs.execute(r'select title from articles where aut_id = ?;', [int(aut_id)])
    return curs.fetchall()


data = parse('biblio.bib')
#fill_authors(data)
#curs.execute(r'select distinct aut_name from authors;')
#print(curs.fetchall())
#fill_articles(data)
#fill_books(data)
#fill_conference(data)
#fill_thesises(data)
#fill_booklets(data)

print(select_by_author('Shliomis, M. I. and Smorodin, B. L.'))