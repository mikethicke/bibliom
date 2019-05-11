"""
Run Crossref queries and return results as publication objects.
"""

import logging
import datetime

import requests
from habanero import crossref

from bibliom.publication_objects import Paper, Author, Journal, Citation
from bibliom.dbentity import DBEntity
from bibliom.dbtable import DBTable

def cr_works(*args, **kwargs):
    """
    Wrapper for crossref query with exception handling. Calls habanero.crossref.Crossref.works().

    See:
        https://github.com/sckott/habanero/blob/master/habanero/crossref/crossref.py
        https://github.com/CrossRef/rest-api-doc#queries
    """
    cr = crossref.Crossref()
    if kwargs.get('timeout') is None:
        kwargs['timeout'] = 10
    try:
        response = cr.works(*args, **kwargs)
    except requests.exceptions.Timeout:
        logging.getLogger(__name__).exception("Timeout from Crossref")
        return None
    except requests.exceptions.ConnectionError:
        logging.getLogger(__name__).exception("Connection error from Crossref")
        return None

    return response

def parse_response(cr_response, existing_paper=None, overwrite=False,
                   min_score=80, manager=None):
    '''
    Parses a crossref response to a Paper object and saves it to db.

    args:
        cr_response:        Crossref response object
        existing_paper:     If set, update existing_paper from response
        overwrite:          If true, overwrite existing_paper fields from response. Otherwise,
                            skip already set fields in existing_paper.
        min_score:          In top Crossref result is below min_score, then return None instead of
                            paper object
        manager:            A DBManager instance

    returns:
        Paper or None
    '''

    try:
        items = cr_response['message'].get('items')
        if items is None:
            top_result = cr_response['message']
        else:
            top_result = items[0]
        score = top_result['score']
    except KeyError:
        raise TypeError("In from_crossref, Not a Crossref response object")

    if score != 1.0 and score < min_score:
        return None

    if existing_paper:
        response_paper = existing_paper
    else:
        response_paper = Paper(manager=manager)
    response_paper.protect_fields = not overwrite

    response_paper.url = top_result.get('URL')
    try:
        response_paper.title = top_result['title'][0]
    except (KeyError, TypeError):
        response_paper.title = top_result.get('title')
    try:
        pages = str(top_result['page']).split('-')
        response_paper.first_page = pages[0]
        response_paper.last_page = pages[1]
    except (KeyError, IndexError):
        pass
    try:
        date_parts = top_result['issued']['date-parts'][0]
        if len(date_parts) == 3:
            p_str = '%Y-%m-%d'
        elif len(date_parts) == 2:
            p_str = '%Y-%m'
        else:
            p_str = '%Y'
        text_date = "-".join(str(x) for x in date_parts)
        response_paper.publication_date = datetime.datetime.strptime(text_date, p_str).date()
    except (KeyError, IndexError):
        pass

    try:
        journal_title = top_result['container-title'][0]
        new_journal = Journal(manager=manager, title=journal_title)
        new_journal.save_to_db()
        response_paper.idjournal = new_journal.idjournal
    except (KeyError, IndexError):
        pass

    response_paper.save_to_db()
    authors = top_result.get('author')
    if authors:
        if response_paper.authors and overwrite:
            keys_to_delete = [author.row_key for author in authors]
            authors[0].table.delete_rows(keys_to_delete)
            
        for author in authors:
            new_author = Author(
                manager=manager,
                last_name=author.get('family'),
                given_names=author.get('given')
            )
            new_author.save_to_db()
            new_paper_author = DBEntity(
                table='paper_author',
                manager=manager,
                idpaper=response_paper.idpaper,
                idauthor=new_author.idauthor
            )
            new_paper_author.save_to_db()
    
    references = top_result.get('reference') or []
    if references:
        if response_paper.cited_papers:
            # To avoid over-counting citattions through duplication, need to remove paper's
            # citations before parsing new ones from Crossref, as it's not guaranteed that
            # a paper parsed from a Crossref citation will necessarily be flagged as a
            # duplicate when being added to the database.
            if overwrite:
                existing_citations = Citation.fetch_entities(
                    table=DBTable.get_table_object('citation', manager=manager),
                    source_id=response_paper.idpaper
                )
                keys_to_delete = [citation.row_key for citation in existing_citations]
                existing_citations[0].table.delete_rows(keys_to_delete)
            else:
                references = [] # Skip for loop

        for reference in references:
            cited_paper = Paper(manager=manager)
            cited_paper.doi = reference.get('DOI')
            cited_paper.title = reference.get('article-title')
            cited_paper.first_page = reference.get('first-page')
            year = reference.get('year')
            if year:
                try:
                    cited_paper.publication_date = datetime.datetime(int(year),1,1).date()
                except ValueError:
                    pass

            cited_journal = Journal(manager=manager)
            cited_journal.title = reference.get('journal-title')
            if cited_journal.title:
                cited_journal.save_to_db()
                cited_paper.idjournal = cited_journal.idjournal

            cited_paper.save_to_db()

            cited_author = Author(manager=manager)
            cited_author.last_name = reference.get('author')

            if cited_author.last_name:
                cited_author.save_to_db()

                cited_paper_author = DBEntity(
                    table='paper_author',
                    manager=manager,
                    idpaper=cited_paper.idpaper,
                    idauthor=cited_author.idauthor 
                )
                cited_paper_author.save_to_db()

            new_citation = Citation(
                manager=manager,
                source_paper=response_paper,
                target_paper=cited_paper)
            new_citation.save_to_db()

    return response_paper

def fill_paper(paper, overwrite=False):
    """
    Fill in missing paper fields for a paper from Crossref query.

    args:
        paper:      Paper to complete.
        overwrite:  If true, overwrite existing fields from Crossref data.

    returns:
        Paper.
    """
    if paper.doi:
        response = cr_works(ids=[paper.doi])
    else:
        field_queries = {}
        if paper.title:
            field_queries['query_title'] = paper.title
        if paper.journal and paper.journal.title:
            field_queries['query_container_title'] = paper.journal.title
        authors = paper.authors
        if authors:
            author_name = "%s+%s" % (
                authors[0].given_names.replace(' ', '+'),
                authors[0].last_name)
            field_queries['query_author'] = author_name
        response = cr_works(**field_queries)

    filled_paper = parse_response(response, paper, overwrite=overwrite)
    return filled_paper
