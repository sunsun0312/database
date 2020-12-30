import ast
import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os

studentid = os.path.basename(sys.modules[__name__].__file__)


#################################################
# Your personal methods can be here ...
# clean data from complex object and connected with ','
def clean_data(column, data, extract):
    for index in data.index:
        value = data[column].loc[index]
        s = json.loads(json.dumps(eval(value)))
        data[column].loc[index] = ','.join(sorted(pd.DataFrame(s)[extract].tolist()))
    return data

# split one row into many rows regarding to some specific value
def split_rows(data, column):
    df = data[column].str.split(',', expand=True).stack()
    df = df.reset_index(level=1, drop=True).rename(column)
    data = data.drop(column, axis=1)
    data = data.join(df)
    return data
#################################################


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))
    if other is not None:
        print(question, other)
    if output_df is not None:
        print(output_df.head(5).to_string())


def question_1(movies, credits):
    """
    :param movies: the path for the movie.csv file
    :param credits: the path for the credits.csv file
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df_credits = pd.read_csv(credits)
    df_movies = pd.read_csv(movies)
#     print(list(df_credits))
#     print(list(df_movies))
    df1 = pd.merge(df_credits, df_movies, how='inner')
    df1.to_csv('test.csv')
    #################################################

    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df2
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    columns = ['id', 'title', 'popularity', 'cast', 'crew', 'budget', 
               'genres', 'original_language', 'production_companies', 
               'production_countries', 'release_date', 'revenue', 
               'runtime', 'spoken_languages', 'vote_average', 'vote_count']
    columns_to_drop = [x for x in df1 if x not in columns ]
#     print(columns_to_drop)
    df2 = df1.drop(columns_to_drop, axis=1)
    #################################################

    log("QUESTION 2", output_df=df2, other=(len(df2.columns), sorted(df2.columns)))
    return df2


def question_3(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df3 = df2.set_index('id')
    #################################################

    log("QUESTION 3", output_df=df3, other=df3.index.name)
    return df3


def question_4(df3):
    """
    :param df3: the dataframe created in question 3
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df4 = df3.query('budget != 0')
    #################################################

    log("QUESTION 4", output_df=df4, other=(df4['budget'].min(), df4['budget'].max(), df4['budget'].mean()))
    return df4


def question_5(df4):
    """
    :param df4: the dataframe created in question 4
    :return: df5
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    success_impact = df4.apply(lambda x: (x['revenue']-x['budget'])/x['budget'], axis=1)
#     print(success_impact)
    df5 = df4.copy()
    df5.insert(0, 'success_impact', success_impact)
    #################################################

    log("QUESTION 5", output_df=df5,
        other=(df5['success_impact'].min(), df5['success_impact'].max(), df5['success_impact'].mean()))
    return df5


def question_6(df5):
    """
    :param df5: the dataframe created in question 5
    :return: df6
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df_popu = df5['popularity']
    df_p = ((df_popu - df_popu.min())/(df_popu.max() - df_popu.min()))* 100
#     print(df_p.min(),df_p.max())
    df6 = df5.copy()
    df6['popularity'] = df_p
    #################################################

    log("QUESTION 6", output_df=df6, other=(df6['popularity'].min(), df6['popularity'].max(), df6['popularity'].mean()))
    return df6


def question_7(df6):
    """
    :param df6: the dataframe created in question 6
    :return: df7
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df7 = df6.astype({'popularity': 'int16'})
    #################################################

    log("QUESTION 7", output_df=df7, other=df7['popularity'].dtype)
    return df7


def question_8(df7):
    """
    :param df7: the dataframe created in question 7
    :return: df8
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df8 = df7.copy()
    df8 = clean_data('cast', df8, 'character')
    #################################################

    log("QUESTION 8", output_df=df8, other=df8["cast"].head(10).values)
    return df8


def question_9(df8):
    """
    :param df9: the dataframe created in question 8
    :return: movies
            Data Type: List of strings (movie titles)
            Please read the assignment specs to know how to create the output
    """

    #################################################
    # Your code goes here ...
    char_num = []
    df9 = df8.copy()
    df9 = split_rows(df9, 'cast')
    m = df9['title']
    count = m.value_counts()
    movies = list(count.index[:10])
    #################################################

    log("QUESTION 9", output_df=None, other=movies)
    return movies


def question_10(df8):
    """
    :param df8: the dataframe created in question 8
    :return: df10
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df10 = df8.copy()
    df10['release_date'] = pd.to_datetime(df10.release_date)
    df10 = df10.sort_values('release_date', ascending=False)
    df10['release_date'] = df10['release_date'].apply(lambda x: '/'.join(str(x)[:10].split('-')[::-1]))
    #################################################

    log("QUESTION 10", output_df=df10, other=df10["release_date"].head(5).to_string().replace("\n", " "))
    return df10


def question_11(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    df11 = df10.copy()
    df11 = clean_data('genres', df11, 'name')

    df11 = split_rows(df11, 'genres')
    genres = df11['genres']
    count = genres.value_counts()
    # just to make sure the genres with small percentage do not get together and make the graph more clear
    count = count.sort_index()

    count.plot.pie(subplots=True, autopct='%1.1f%%',figsize=(10,8),pctdistance=0.9, radius=1.5,
                       textprops = {'fontsize':10}, labeldistance=1.05)

    plt.legend(bbox_to_anchor=(1,0.5), loc="center right", fontsize=15, bbox_transform=plt.gcf().transFigure)
    plt.subplots_adjust(left=0.0, bottom=0.1, right=0.55)

    plt.ylabel('')
    plt.title('Genres', fontsize=25, pad = 80)
    plt.tight_layout()
   
    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_12(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    plt.close()
    df12 = df10.copy()
    df12 = clean_data('production_countries', df12, 'name')
    df12 = split_rows(df12, 'production_countries')
    countries = df12['production_countries']
    count = countries.value_counts()
    count = count.sort_index()
    count.plot.bar(figsize=(17,8), fontsize=15, color='blue')
    plt.title('production countries', fontsize=30, pad=30)
    plt.xlabel('country name', fontsize=15)
    plt.ylabel('movie number', fontsize=15)
    plt.tight_layout()
    

    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_13(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    plt.close()

    df13 = df10.copy()

    df = df13.groupby('original_language')
    fig, ax = plt.subplots(figsize=(15,8))
    for index, group in df:
        ax.scatter(group['vote_average'], group['success_impact'], label = index, s=150)
        
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0,numpoints=1,fontsize=20)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    ax.set_xlabel('vote average', fontsize=20)
    ax.set_ylabel('success impact', fontsize=20)
    ax.set_title('vote_average vs success_impact', fontsize=30, pad=40)
    plt.tight_layout()
    
    #################################################

    plt.savefig("{}-Q13.png".format(studentid))



if __name__ == "__main__":
    df1 = question_1("movies.csv", "credits.csv")
    df2 = question_2(df1)
    df3 = question_3(df2)
    df4 = question_4(df3)
    df5 = question_5(df4)
    df6 = question_6(df5)
    df7 = question_7(df6)
    df8 = question_8(df7)
    movies = question_9(df8)
    df10 = question_10(df8)
    question_11(df10)
    question_12(df10)
    question_13(df10)
