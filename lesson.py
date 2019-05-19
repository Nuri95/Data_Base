import pprint
import sqlite3 as lite
import sys
import pickle

con = None

query_string_first_task = '''
select CustomerId, FirstName, Company, Phone, Email from Customer as c
where 50 < (select 2019 - BirthDate from Employee as e where e.EmployeeId == c.SupportRepId) and
      exists(select t.GenreId
             from (Invoice
              inner join InvoiceLine on Invoice.InvoiceId = InvoiceLine.InvoiceId
              inner join Track on InvoiceLine.TrackId = Track.TrackId) t
          where c.CustomerId = t.CustomerId and t.GenreId != (select GenreId from Genre where Name == 'Rock'))
order by c.City, c.Email DESC limit 10


'''

query_string_second_task = '''
select FirstName, Phone, ReportsTo from Employee where ReportsTo
'''

query_string_third_task = '''
select FirstName, Phone from Customer as c
where exists(
    select t.TrackId
    from (Invoice inner join InvoiceLine on Invoice.InvoiceId = InvoiceLine.InvoiceId) t
    where t.CustomerId = c.CustomerId and t.TrackId in (
        select TrackId
        from Track where UnitPrice = (select max(UnitPrice) from Track))
    )
'''


def db_output(query_string):
    try:
        con = lite.connect('Chinook_Sqlite.sqlite')
        # создаем объект cursor, который позволяет нам взаимодействовать с базой данных и добавлять записи
        cur = con.cursor()
        cur.execute(query_string)  # Вставляем данные в таблицу

        con.commit()   # # Сохраняем изменения
        data = cur.fetchall()   # для получения результатов
        pprint.pprint(data)
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        if con is not None:
            con.close()
    return data


def db_write(query_string):
    try:
        with open('pickle.data', 'wb') as f_obj:
            pickle.dump(query_string, f_obj)
    except IOError as io:
        print(io)


db_output(query_string_first_task)
db_write(query_string_second_task)
db_output(query_string_third_task)
