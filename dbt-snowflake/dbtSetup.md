# DBT desktop

<!-- https://docs.getdbt.com/dbt-cli/install/pip -->
## [Install](https://docs.getdbt.com/dbt-cli/install/pip) dbt in virtualenv
```
mkdir dbt
cd dbt
virtualenv venv
source venv/bin/activate
pip install dbt-snowflake
```



## Initialize project:
```dbt init <project_name> ```

## Naming Conventions:
- Sources - (src) raw table in dwh
- Staging - (stg) models built directly on top of sources
- Intermediate - (int) any models betwwen fct and dim tables, should be built on stg's
- Fact - (fct) data that represents things that are occurring/occurred
- Dimension - (dim)  data that represent a person/place/thing

# Snowflake CLI
## [Install](https://docs.snowflake.com/en/user-guide/snowsql-install-config.html#installing-snowsql-on-macos-using-homebrew-cask) Snowflake Cli
```
brew install --cask snowflake-snowsql
```
open (or [create](https://osxdaily.com/2021/11/18/where-the-zshrc-file-is-located-on-mac/)) ~/.zshrc
```
touch ~/.zshrc
```
add the following, and save:
```
alias snowsql=/Applications/SnowSQL.app/Contents/MacOS/snowsql
```
## [Connect](https://docs.snowflake.com/en/user-guide/snowsql-start.html)
Using a Web Browser for Federated Authentication/SSO:
```
snowsql -a <account_identifier> -u <username> --authenticator externalbrowser
snowsql -a ada43147.us-east-1 -u jszot@joinpapa.com --authenticator externalbrowser
```
## [Disconnect](https://docs.snowflake.com/en/user-guide/snowsql-use.html#disconnecting-from-snowflake-and-stopping-snowsql)
run ```!exit```


# Courses:
- [Fundamentals](https://courses.getdbt.com/courses/fundamentals)

