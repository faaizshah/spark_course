To populate a PostgreSQL database with the sample "DVD Rental" database from the PostgreSQL tutorial, follow these steps:


2. **Upload the database dump file**
   Get the ID of the instance then run 
   ```bash
    kubectl -n spark-do5 cp ./data/dvdrental.tar postgresql-<ID to get>:/bitnami/postgresql
   ```

4. **Restore the database from the dump file**
Use the `pg_restore` utility to restore the database from the extracted `dvdrental.tar` file. Run the following command, replacing `<username>` with your PostgreSQL user:

```
kubectl exec -ti postgresql-<ID to get> -- /bin/bash
cd /bitnami.postgres 
pg_restore -d dvdrental dvdrental.tar
```

the username is ``postgres`` with password ``password``

This will populate the `dvdrental` database with all the tables, data, views, functions, and other objects from the sample database.

You can then use the ``PG_Yann_Pomie`` object to query the database

Citations:
[1] https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/
[2] https://github.com/guenthermi/the-movie-database-import
[3] https://github.com/transitive-bullshit/populate-movies
[4] https://forum.obsidian.md/t/create-a-personal-movie-database-using-dataview-quickadd-and-minimal-theme/37365
[5] https://1kevinson.com/how-to-create-a-postgres-database-in-docker/
[6] https://www.coursehero.com/file/23211726/IT234-KatelynKunz-Unit7/