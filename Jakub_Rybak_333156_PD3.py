import sqlite3
import pandas as pd
import copy


Posts = pd.read_csv("./travel_stackexchange_com/Posts.csv.gz",
compression = 'gzip')
Comments = pd.read_csv("./travel_stackexchange_com/Comments.csv.gz",
compression = 'gzip')
PostLinks = pd.read_csv("./travel_stackexchange_com/PostLinks.csv.gz",
compression = 'gzip')
Users = pd.read_csv("./travel_stackexchange_com/Users.csv.gz",
compression = 'gzip')
Votes = pd.read_csv("./travel_stackexchange_com/Votes.csv.gz",
compression = 'gzip')


SCIEZKA_BAZY = './pd5_baza.db'


with sqlite3.connect(SCIEZKA_BAZY) as conn: # połączenie do bazy danych

    Comments.to_sql("Comments", conn, if_exists='replace')
    Posts.to_sql("Posts", conn, if_exists='replace')
    PostLinks.to_sql("PostLinks", conn, if_exists='replace')
    Users.to_sql("Users", conn, if_exists='replace')
    Votes.to_sql("Votes", conn, if_exists='replace')

#Grupujemy dane z tabeli Users po roku i miesiącu utworzenia każdego konta,
#następnie obliczamy liczbę wszystkich kont oraz średnią reputację użytkowników

zapytanie_1 = """
SELECT STRFTIME('%Y', CreationDate) AS Year, 
       STRFTIME('%m', CreationDate) AS Month, 
       COUNT(*) AS TotalAccountsCount, 
       AVG(Reputation) AS AverageReputation
FROM Users
GROUP BY Year, Month
"""

#Wybieramy dane użytkowników wraz z ich lokalizacją, reputacją, datą utworzenia konta oraz sumaryczną liczbą komentarzy do odpowiedzi,
#sortujemy wyniki malejąco według sumarycznej liczby komentarzy i ograniczamy do 10 wyników.

zapytanie_2 = """
SELECT Users.DisplayName, Users.Location, Users.Reputation, 
       STRFTIME('%Y-%m-%d', Users.CreationDate) AS CreationDate,
       Answers.TotalCommentCount
FROM (
        SELECT OwnerUserId, SUM(CommentCount) AS TotalCommentCount
        FROM Posts
        WHERE PostTypeId == 2 AND OwnerUserId != ''
        GROUP BY OwnerUserId
     ) AS Answers
JOIN Users ON Users.Id == Answers.OwnerUserId
ORDER BY TotalCommentCount DESC
LIMIT 10
"""

#Wybieramy identyfikator postu, typ postu, wynik, identyfikator właściciela, nazwę wyświetlaną użytkownika i reputację
#łącząc posty oznaczone jako spam z danymi użytkowników, którzy stworzyli te posty.

zapytanie_3 = """
SELECT Spam.PostId, UsersPosts.PostTypeId, UsersPosts.Score, 
       UsersPosts.OwnerUserId, UsersPosts.DisplayName,
       UsersPosts.Reputation
FROM (
        SELECT PostId  
        FROM Votes
        WHERE VoteTypeId == 12
     ) AS Spam
JOIN (
        SELECT Posts.Id, Posts.OwnerUserId, Users.DisplayName, 
               Users.Reputation, Posts.PostTypeId, Posts.Score
        FROM Posts JOIN Users
        ON Posts.OwnerUserId = Users.Id
     ) AS UsersPosts 
ON Spam.PostId = UsersPosts.Id
"""

#Wybieramy identyfikator użytkownika, jego nazwę wyświetlaną, liczbę głosów za i przeciw, reputację oraz liczbę zduplikowanych pytań,
#łącząc dane zduplikowanych postów z danymi użytkowników, którzy stworzyli te posty,
#grupując wyniki według identyfikatora użytkownika i wybierając tylko tych, którzy mają więcej niż 100 zduplikowanych pytań,
#sortując wyniki malejąco według liczby zduplikowanych pytań.

zapytanie_4 = """
SELECT Users.Id, Users.DisplayName, Users.UpVotes, Users.DownVotes, Users.Reputation,
       COUNT(*) AS DuplicatedQuestionsCount
FROM (
        SELECT Duplicated.RelatedPostId, Posts.OwnerUserId
        FROM (
                SELECT PostLinks.RelatedPostId
                FROM PostLinks
                WHERE PostLinks.LinkTypeId == 3
             ) AS Duplicated
        JOIN Posts
        ON Duplicated.RelatedPostId = Posts.Id
     ) AS DuplicatedPosts
JOIN Users ON Users.Id == DuplicatedPosts.OwnerUserId
GROUP BY Users.Id
HAVING DuplicatedQuestionsCount > 100
ORDER BY DuplicatedQuestionsCount DESC
"""

#Wybieramy identyfikator pytania, jego tytuł, wynik, maksymalny wynik zduplikowanego pytania, liczbę zduplikowanych wystąpień oraz okres dnia,
#łącząc dane pytania z informacjami o zduplikowanych pytaniach, grupując wyniki według identyfikatora pytania i sortując malejąco według liczby zduplikowanych wystąpień.

zapytanie_5 = """
SELECT QuestionsAnswers.Id,
       QuestionsAnswers.Title, 
       QuestionsAnswers.Score,
       MAX(Duplicated.Score) AS MaxScoreDuplicated,
       COUNT(*) AS DulicatesCount,
       CASE 
         WHEN QuestionsAnswers.Hour < '06' THEN 'Night'
         WHEN QuestionsAnswers.Hour < '12' THEN 'Morning'
         WHEN QuestionsAnswers.Hour < '18' THEN 'Day'
         ELSE 'Evening'
         END DayTime
FROM (
        SELECT Id, Title, 
               STRFTIME('%H', CreationDate) AS Hour, Score 
        FROM Posts
        WHERE Posts.PostTypeId IN (1, 2)
     ) AS QuestionsAnswers
JOIN (
        SELECT PL3.RelatedPostId, Posts.Score
        FROM (
               SELECT RelatedPostId, PostId
               FROM PostLinks
               WHERE LinkTypeId == 3
             ) AS PL3
        JOIN Posts ON PL3.PostId = Posts.Id
     ) AS Duplicated
ON QuestionsAnswers.Id = Duplicated.RelatedPostId
GROUP BY QuestionsAnswers.Id
ORDER By DulicatesCount DESC
"""


with sqlite3.connect(SCIEZKA_BAZY) as conn:
    sql_1 = pd.read_sql_query(zapytanie_1, conn)
    sql_2 = pd.read_sql_query(zapytanie_2, conn)
    sql_3 = pd.read_sql_query(zapytanie_3, conn)
    sql_4 = pd.read_sql_query(zapytanie_4, conn)
    sql_5 = pd.read_sql_query(zapytanie_5, conn)


try:
    import copy
    pandas_1 = copy.deepcopy(Users)
    # Konwersja kolumny CreationDate na datetime
    pandas_1['CreationDate'] = pd.to_datetime(pandas_1['CreationDate'])

    # Wyciągnij rok i miesiąc z kolumny CreationDate
    pandas_1['Year'] = pandas_1['CreationDate'].dt.year.astype(str)
    pandas_1['Month'] = pandas_1['CreationDate'].dt.month.astype(str).str.zfill(2)

    # Grupowanie danych według roku i miesiąca
    pandas_1 = pandas_1.groupby(['Year', 'Month']).agg(
        TotalAccountsCount=('Id', 'size'),
        AverageReputation=('Reputation', 'mean')
    ).reset_index()

    print(pandas_1.equals(sql_1))


except Exception as e:
    print("Zad. 1: niepoprawny wynik.")
    print(e)


try:

    # Wybierz odpowiednie posty (PostTypeId == 2 i OwnerUserId niepuste)
    answers = Posts[(Posts['PostTypeId'] == 2) & (Posts['OwnerUserId'] != '')]

    # Grupowanie postów według OwnerUserId i obliczenie sumy komentarzy dla każdego użytkownika
    answers = answers.groupby('OwnerUserId').agg(
        TotalCommentCount=('CommentCount', 'sum')
    )

    # Dołącz dane o użytkownikach do danych o odpowiedziach
    pandas_2 = pd.merge(answers, Users, left_on='OwnerUserId', right_on='Id')

    # Wybierz odpowiednie kolumny i przekształć CreationDate do żądanego formatu
    pandas_2['CreationDate'] = pd.to_datetime(pandas_2['CreationDate']).dt.strftime('%Y-%m-%d')

    # Wybierz potrzebne kolumny
    pandas_2 = pandas_2[['DisplayName', 'Location', 'Reputation', 'CreationDate', 'TotalCommentCount']]

    # Posortuj wynik malejąco według TotalCommentCount i wybierz 10 pierwszych rekordów
    pandas_2 = pandas_2.sort_values(by='TotalCommentCount', ascending=False).head(10).reset_index(drop=True)

    print(pandas_2.equals(sql_2))

except Exception as e:
    print("Zad. 2: niepoprawny wynik.")
    print(e)


try:
    # Krok 1: Filtracja tabeli votes dla VoteTypeId == 12
    spam = Votes[Votes['VoteTypeId'] == 12][['PostId']]

    # Krok 2: Połączenie tabel posts i users na podstawie OwnerUserId
    users_posts = Posts.merge(Users, left_on='OwnerUserId', right_on='Id', suffixes=('', '_User'))
    users_posts = users_posts[['Id', 'PostTypeId', 'Score', 'OwnerUserId', 'DisplayName', 'Reputation']]

    # Krok 3: Połączenie spam i users_posts na podstawie PostId (Spam.PostId == UsersPosts.Id)
    pandas_3 = spam.merge(users_posts, left_on='PostId', right_on='Id', how='inner')

    # Usunięcie kolumny 'Id', ponieważ nie jest potrzebna w końcowym wyniku
    pandas_3 = pandas_3.drop(columns=['Id'])

    print(pandas_3.equals(sql_3))


except Exception as e:
    print("Zad. 3: niepoprawny wynik.")
    print(e)

try:
    # Krok 1: Filtracja tabeli PostLinks dla LinkTypeId == 3
    duplicated = PostLinks[PostLinks['LinkTypeId'] == 3][['RelatedPostId']]

    # Krok 2: Połączenie tabeli duplicated z tabelą Posts na podstawie RelatedPostId
    duplicated_posts = duplicated.merge(Posts, left_on='RelatedPostId', right_on='Id')[['RelatedPostId', 'OwnerUserId']]

    # Krok 3: Połączenie duplicated_posts z tabelą Users na podstawie OwnerUserId
    pandas_4 = duplicated_posts.merge(Users, left_on='OwnerUserId', right_on='Id')

    # Krok 4: Grupowanie według Users.Id i liczenie liczby zduplikowanych postów dla każdego użytkownika
    pandas_4 = pandas_4.groupby(
        ['OwnerUserId', 'DisplayName', 'UpVotes', 'DownVotes', 'Reputation']).size().reset_index(
        name='DuplicatedQuestionsCount')

    # Krok 5: Filtracja użytkowników, którzy mają więcej niż 100 zduplikowanych postów

    pandas_4 = pandas_4[pandas_4['DuplicatedQuestionsCount'] > 100]

    # Krok 6: Sortowanie według DuplicatedQuestionsCount malejąco
    pandas_4 = pandas_4.sort_values(by='DuplicatedQuestionsCount', ascending=False).reset_index(drop=True)
    pandas_4['OwnerUserId'] = pandas_4['OwnerUserId'].astype("int64")
    pandas_4 = pandas_4.rename(columns={"OwnerUserId": "Id"})

    print(pandas_4.equals(sql_4))


except Exception as e:
    print("Zad. 4: niepoprawny wynik.")
    print(e)

try:
    import copy
    Posts1 = copy.deepcopy(Posts)
    # Filtrowanie i przetwarzanie tabeli Posts
    questions_answers = Posts1[Posts['PostTypeId'].isin([1, 2])]
    questions_answers['Hour'] = pd.to_datetime(questions_answers['CreationDate']).dt.hour
    questions_answers = questions_answers[['Id', 'Title', 'Hour', 'Score']]

    # Tworzenie tabeli Duplicated
    pl3 = PostLinks[PostLinks['LinkTypeId'] == 3][['RelatedPostId', 'PostId']]
    duplicated = pl3.merge(Posts[['Id', 'Score']], left_on='PostId', right_on='Id')
    duplicated = duplicated[['RelatedPostId', 'Score']]

    # Połączenie tabel QuestionsAnswers i Duplicated
    pandas_5 = questions_answers.merge(duplicated, left_on='Id', right_on='RelatedPostId',
                                     suffixes=('_questions_answers', '_duplicated'))

    # Grupowanie i agregacja
    pandas_5 = pandas_5.groupby(['Id', 'Title', 'Score_questions_answers', 'Hour']).agg(
        MaxScoreDuplicated=pd.NamedAgg(column='Score_duplicated', aggfunc='max'),
        DulicatesCount=pd.NamedAgg(column='Score_duplicated', aggfunc='count')
    ).reset_index()


    # Dodanie kolumny DayTime
    def get_day_time(hour):
        if hour < 6:
            return 'Night'
        elif hour < 12:
            return 'Morning'
        elif hour < 18:
            return 'Day'
        else:
            return 'Evening'


    pandas_5['DayTime'] = pandas_5['Hour'].apply(get_day_time)

    # Sortowanie wyników
    pandas_5 = pandas_5.sort_values(by='DulicatesCount', ascending=False).reset_index(drop=True)

    pandas_5 = pandas_5.rename(columns=({"Score_questions_answers": "Score"}))
    pandas_5 = pandas_5.drop(columns=['Hour'])

    #jak posortujemy po id, gdzie id jest unikatowe to sie wszystko zgadza
    #pandas_5 = pandas_5.sort_values(by="Id").reset_index(drop=True)
    #sql_5 = sql_5.sort_values(by="Id").reset_index(drop=True)

    print(pandas_5.equals(sql_5))

except Exception as e:
    print("Zad. 5: niepoprawny wynik.")
    print(e)

