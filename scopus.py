import pandas as pd
from sqlalchemy import create_engine

def preprocess(data):
    
    """
    takes the csv file and process it and trnasforme it to multiple dataframe uploadable
    to the data warehouse
    
    BY Badr Eddine Jalili
    """
    
      #cleaning
    drop_index = []
    for _,row in data.iterrows():
        if len(str(row['Authors with affiliations']).split(';'))!=len(str(row['Author(s) ID']).split(';')):
              drop_index.append(_)

      #drop row in which authors count and authors with affiliation count doesnt match
    for index in drop_index:
        data.drop(index,inplace=True)

      #drop null values
    data.dropna(subset=['Authors', 'Author full names', 'Author(s) ID', 'Title', 'Year',
            'DOI', 'Link', 'Affiliations','Authors with affiliations','EID'], inplace=True)


      #create affiliation dataframe
    aff_list = []
    country_list = []

    for affili_row in data['Authors with affiliations']:
        for aff in affili_row.split(';'):
            if len(aff.split(','))<3:
                aff_list.append('unknown')
                country_list.append('unkown')
            else:
                aff_list.append(aff.split(',')[1].strip())
                country_list.append(aff.split(',')[-1].strip())

    affiliation_data = pd.DataFrame({'affiliate':aff_list,'country':country_list})

      #drop duplicate and add id to affiliation dataframe
    affiliation_data.drop_duplicates(subset=['affiliate'],inplace = True)
    affiliation_id = [i+1 for i in range(affiliation_data.shape[0])]
    affiliation_data['affiliation_id']= affiliation_id
    
      #create author dataframe
    author_name = []
    author_id = []
    author_aff = []
    for authors in data['Author full names']:
        for author in authors.split(';'):
            author_name.append(author.split('(')[0].strip())
            author_id.append(int(author.split('(')[-1].strip()[:-1]))
    for affili_row in data['Authors with affiliations']:
        for aff in affili_row.split(';'):
            if len(aff.split(','))<3:
                author_aff.append(int(affiliation_data[affiliation_data['affiliate']=='unknown']['affiliation_id']))
            else:
                author_aff.append(int(affiliation_data[affiliation_data['affiliate']==aff.split(',')[1].strip()]['affiliation_id']))
    author_data = pd.DataFrame({'author_id':author_id,'author_name':author_name,'affiliation_id':author_aff})

      #drop duplicate and add id to author dataframe
    author_data.drop_duplicates(subset=['author_id'],inplace=True)

      # create article_id column
    data['article_id']=data['EID'].apply(lambda x:x.split('-')[-1])


      #create meta_data dataframe
    article_id=[]
    Author_id=[]
    Affiliation=[]
    affiliation_id=[]
    author_id=[]
    Author_kw=[]
    Document_Type=[]
    DOI=[]
    Index_kw=[]
    ISSN=[]
    Source=[]
    Title=[]
    Year=[]

    for ind,row in data.iterrows():
        for author in row['Author(s) ID'].split(';'):
            article_id.append(row['article_id'])
            Author_id.append(int(author.strip()))
            affiliation_id.append(author_data[author_data['author_id']==int(author.strip())].affiliation_id.values[0])
            Affiliation.append(affiliation_data[affiliation_data['affiliation_id']==affiliation_id[-1]]['affiliate'].values[0])
            Author_kw.append(row['Author Keywords'])
            Document_Type.append(row['Document Type'])
            DOI.append(row['DOI'])
            Index_kw.append(row['Index Keywords'])
            ISSN.append(row['ISSN'])
            Source.append(row['Source'])
            Title.append(row['Title'])
            Year.append(row['Year'])
    meta_data = pd.DataFrame({'affiliation':Affiliation,'affiliation_id':affiliation_id,
                              'article_id':article_id,'author_id':Author_id,
                              'author_kw':Author_kw,'document_type':Document_Type,
                              'DOI':DOI,'index_kw':Index_kw,'ISSN':ISSN,'source':Source,
                              'title':Title,'year':Year})
    return author_data,affiliation_data,meta_data



def upload(author_data, affiliation_data, meta_data, option):

    db_params = {
        "dbname": "sdgs",
        "user": "postgres",
        "password": "123",
        "host": "localhost",
        "port": "3306"  # The default PostgreSQL port is 5432
    }

    # Create the connection engine
    db_connection = create_engine(
    f'mysql+mysqlconnector://{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}'
)

    # Upload dataframes to PostgreSQL tables
    affiliation_data.to_sql('Affiliation', db_connection, if_exists=option, index=False)
    author_data.to_sql('author', db_connection, if_exists=option, index=False)
    meta_data.to_sql('metadata', db_connection, if_exists=option, index=False)

    # Dispose of the database connection
    db_connection.dispose()

# Example usage:
# upload(author_data, affiliation_data, meta_data, option='replace')
