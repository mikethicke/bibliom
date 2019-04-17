"""
Run Crossref queries and return results as publication objects.
"""

from habanero import crossref
from publication_objects import Paper

def parse_crossref_response(cr_response, min_score = 80) :
        '''
        Parses a crossref response to a Paper.
        
        args:
            cr_response : Crossref response object
            min_score : In top Crossref result is below min_score, then return None instead of
                        paper object
        
        returns:
            True if a result of at least min_score is successfully parsed
        '''
        
        try :
            items = cr_response['message'].get('items')
            if items is None: 
                top_result = cr_response['message']
            else:
                top_result = items[0]
            score = top_result['score']
        except: #TODO: specify exception type 
            log.error("In from_crossref, Not a Crossref response object")
            raise
        
        if score != 1.0 and score < min_score: 
            return None
        
        new_paper 
        
        self.url = top_result.get('URL')
        try :
            self.title = top_result['title'][0]
        except :
            self.title = top_result.get('title')
        try :
            self.journal.short_title = top_result['short-container-title'][0]
        except :
            pass
        try :
            self.journal.title = top_result['container-title'][0]
        except :
            pass
        try :
            pages = str(top_result['page']).split('-')
            self.first_page = pages[0]
            self.last_page = pages[1]
        except :
            pass
        
        authors = top_result.get('author')
        if authors is not None:
            for author in authors :
                new_author = Author(last_name = author.get('family'),
                                    given_names= author.get('given'))
                if new_author.last_name is not None and new_author.given_names is not None :
                    self.authors.append(new_author)
        
        try :
            date_parts = top_result['issued']['date-parts'][0]
            if len(date_parts) == 3 :
                p_str = '%Y-%m-%d'
            elif len(date_parts) == 2 :
                p_str = '%Y-%m'
            else :
                p_str = '%Y'
            text_date = "-".join(str(x) for x in date_parts)
            self.publication_date = datetime.datetime.strptime(text_date, p_str).date()
        except Exception as e:
            log.debug("Error parsing date from Crossref. " + str(e))
        
        
        if not shallow :
            for reference in (top_result.get('reference') or []) :
                new_citation = Citation()
                new_citation.source_paper = self
                new_citation.cited_paper = type(self)()
                new_citation.cited_paper.discovery_method.method = "cited"
                new_citation.cited_paper.discovery_source = self

                new_citation.cited_paper.doi = reference.get('DOI')
                score = 0
                if new_citation.cited_paper.doi is None:
                    score = new_citation.cited_paper.from_citation(reference.get('unstructured'))
                if score < min_score :
                    new_citation.cited_paper.authors.append(Author(reference.get('author')))
                    j_title = reference.get('journal-title')
                    if j_title is not None :
                        if '.' in j_title : new_citation.cited_paper.journal.short_title = j_title
                        else : new_citation.cited_paper.journal.title = j_title
                    new_citation.cited_paper.first_page = reference.get('first-page')
                    new_citation.cited_paper.title = reference.get('article-title')
                    
                    year = reference.get('year')
                    if year != None :
                        try :
                            new_citation.cited_paper.publication_date = datetime.datetime(int(year),1,1).date()
                        except :
                            pass
                self.citations.append(new_citation)
                
        return True