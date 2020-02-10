# -- APAN 5310: SQL & RELATIONAL DATABASES FALL 2019 (F2F)

#   -------------------------------------------------------------------------
#   --                                                                     --
#   --                            HONOR CODE                               --
#   --                                                                     --
#   --  I affirm that I will not plagiarize, use unauthorized materials,   --
#   --  or give or receive illegitimate help on assignments, papers, or    --
#   --  examinations. I will also uphold equity and honesty in the         --
#   --  evaluation of my work and the work of others. I do so to sustain   --
#   --  a community built around this Code of Honor.                       --
#   --                                                                     --
#   -------------------------------------------------------------------------


#     You are responsible for submitting your own, original work. We are
#     obligated to report incidents of academic dishonesty as per the
#     Student Conduct and Community Standards.


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


# -- HOMEWORK ASSIGNMENT 6 (DUE 11/12/2019, 11:59 pm EST)


#   NOTES:
#
#     - Type your code between the START and END tags. Code should cover all
#       questions. Do not alter this template file in any way other than
#       adding your answers. Do not delete the START/END tags. The file you
#       submit will be validated before grading and will not be graded if it
#       fails validation due to any alteration of the commented sections.
#
#     - Our course is using PostgreSQL which has been preinstalled for you in
#       Codio. We grade your assignments in Codio and PostgreSQL. You risk
#       losing points if you prepare your SQL queries for a different database
#       system (MySQL, MS SQL Server, Oracle, etc).
#
#     - Make sure you test each one of your answers. If a query returns an error
#       it will earn no points.
#
#     - In your CREATE TABLE statements you must provide data types AND
#       primary/foreign keys (if applicable).
#
#     - You may expand your answers in as many lines as you find appropriate
#       between the START/END tags.
#


# -----------------------------------------------------------------------------


#
#  NOTE: Provide the script that covers all questions between the START/END tags
#        at the end of the file. Separate START/END tags for each question.
#        Add comments to explain each step.
#
#
#  QUESTION 1 (5 points)
#  ---------------------
#  For this assignment we will use a fictional dataset of customer transactions
#  of a pharmacy. The dataset provides information on customer purchases of
#  one or two drugs at different quantities. Download the dataset from the
#  assignment page.
#
#  You will notice that there can be multiple transactions per customer, each
#  one recorded on a separate row.
#
#  Design an appropriate 3NF relational schema. Then, create a new database
#  called "abc_pharmacy" in pgAdmin. Finally, provide the R code that
#  connects to the new database and creates all necessary tables as per the 3NF
#  relational schema you designed (Note: you should create more than one table).
#
#  NOTE: You should use pgAdmin ONLY to create the database. All other actions
#        must be performed in your R code. No points if the database
#        tables are created in pgAdmin and not with R code.

# -- START R CODE --

require('RPostgreSQL')
df <- read.csv('APAN5310f19_HW6_DATA.csv')
head(df)

# Load the PostgreSQL driver
drv <- dbDriver('PostgreSQL')

# Create a connection
con <- dbConnect(drv, dbname = 'abc_pharmacy',
                 host = 'f19server.apan5310.com', port = 50201,
                 user = 'postgres', password = '4rabf037')

stmt <- "
    CREATE TABLE customer_table (
        customer_id     integer,
        first_name        varchar(50) NOT NULL,
        last_name         varchar(50) NOT NULL,
        email                varchar(50) UNIQUE NOT NULL,
        PRIMARY KEY (customer_id)
    );

CREATE TABLE customer_contact_table (
        customer_id       integer,
        phone_number   varchar(20),
        phone_type         varchar(4),
        PRIMARY KEY (customer_id, phone_number),
       FOREIGN KEY (customer_id) REFERENCES customer_table (customer_id),
       CHECK (phone_type IN ('home', 'cell'))
    );

    CREATE TABLE drug_company_table (
        company_id      integer,
        company_name   varchar(100),
        PRIMARY KEY (company_id)
    );
    
    CREATE TABLE drug_table (
        drug_id          integer,
        drug_name    varchar(110),
        PRIMARY KEY (drug_id)
    );
    
    CREATE TABLE drug_company_offerings_table (
        company_id    integer,
        drug_id           integer,
        PRIMARY KEY (company_id, drug_id),
        FOREIGN KEY (company_id) REFERENCES drug_company_table (company_id),
        FOREIGN KEY (drug_id) REFERENCES drug_table (drug_id)
    );
    
    CREATE TABLE purchases_table (
        customer_id      integer,
       purchase_timestamp   timestamp,
        company_id      integer,
        drug_id             integer, 
        quantity            integer,
        price               numeric(5,2),          
        PRIMARY KEY (customer_id, purchase_timestamp, company_id, drug_id),
        FOREIGN KEY (customer_id) REFERENCES customer_table (customer_id),
        FOREIGN KEY (company_id) REFERENCES drug_company_table (company_id),
        FOREIGN KEY (drug_id) REFERENCES drug_table (drug_id)
    );
"

dbGetQuery(con, stmt)


# -- END R CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 2 (15 points)
#  ---------------------
#  Provide the R code that populates the database with the data from the
#  provided "APAN5310f19_HW6_DATA.csv" file. You can download the dataset
#  from the assignment page. It is anticipated that you will perform several steps
#  of data processing in R in order to extract, transform and load all data from
#  the file to the database tables. Manual transformations in a spreadsheet, or
#  similar, are not acceptable, all work must be done in R. Make sure your code
#  has no errors, no partial credit for code that returns errors. When grading,
#  we will run your script and test your work with sample SQL scripts on the
#  database that will be created and populated.
#

# -- START R CODE --

#################### populate customer_table ######################




#Check if duplicates 
df[duplicated(df[c('first_name', 'last_name')]),]

# Remove duplicates based on customer_id columns 
Cust<-df[c('first_name', 'last_name', 'email')]
Cust <-Cust[!duplicated(Cust$email), ]

#Check if removed
Cust[duplicated(Cust[c('first_name', 'last_name')]),]
Cust$customer_id <- 1:nrow(Cust)

head(Cust)

customers_df <- Cust[c('customer_id', 'first_name', 'last_name', 'email')]

head(customers_df)

dbWriteTable(con, name="customer_table", value=customers_df, row.names=FALSE, append=TRUE)

# Map customer_id
customer_id_list <- sapply(df$email, function(x) Cust$customer_id[Cust$email == x])
                        
# Addcustomer_id to the main dataframe
df$customer_id <- customer_id_list

#check it
df[c('first_name', 'last_name', 'email','customer_id')][df$email %in% c('tdewolfer1@smugmug.com'),]

################### populate drug_company_table #######################
temp_drug_comp_1 <-  df[c('drug_company_1')]
colnames(temp_drug_comp_1)[colnames(temp_drug_comp_1)=="drug_company_1"] <- "company_name"
temp_drug_comp_2 <- df[c('drug_company_2')]
colnames(temp_drug_comp_2)[colnames(temp_drug_comp_2)=="drug_company_2"] <- "company_name"
drug_comp<- rbind(temp_drug_comp_1,temp_drug_comp_2)
# Create temporary dataframe with unique movie titles
temp_drug_comp_df <- data.frame('company_name' = unique(drug_comp$company_name))

# Add incrementing integers
temp_drug_comp_df$company_id <- 1:nrow(temp_drug_comp_df)
head(temp_drug_comp_df)

dbWriteTable(con, name="drug_company_table", value=temp_drug_comp_df, row.names=FALSE, append=TRUE)



###################################### populate drug_table ########

temp_drug_name_1 <-  df[c('drug_name_1')]
colnames(temp_drug_name_1)[colnames(temp_drug_name_1)=="drug_name_1"] <- "drug_name"
temp_drug_name_2 <- df[c('drug_name_2')]
colnames(temp_drug_name_2)[colnames(temp_drug_name_2)=="drug_name_2"] <- "drug_name"
drug_nm<- rbind(temp_drug_name_1,temp_drug_name_2)
# Create temporary dataframe with unique movie titles
temp_drug_name_df <- data.frame('drug_name' = unique(drug_nm$drug_name))

# Add incrementing integers
temp_drug_name_df$drug_id <- 1:nrow(temp_drug_name_df)
head(temp_drug_name_df)
dbWriteTable(con, name="drug_table", value=temp_drug_name_df, row.names=FALSE, append=TRUE)

######################## populate customer_contact_table ###############
library(dplyr)
library(tidyr)
cs <- df[c('customer_id', 'cell_and_home_phones')]
cs<-df[!duplicated(df[c('customer_id', 'cell_and_home_phones')]),]
cs[duplicated(cs[c('customer_id', 'cell_and_home_phones')]),]
cs1 <- separate(cs, cell_and_home_phones, c("cell", "home"), ";")
cell <- cs1[c('customer_id', 'cell')]
cell$phone_type <- rep('cell',nrow(cell)) 
colnames(cell)[colnames(cell)=="cell"] <- "phone_number"

home <- cs1[c('customer_id', 'home')]
home$phone_type <- rep('home',nrow(home)) 
colnames(home)[colnames(home)=="home"] <- "phone_number"



phones<- rbind(cell, home)
dbWriteTable(con, name="customer_contact_table", value=phones, row.names=FALSE, append=TRUE)

############################################ populate purchases_table


A<-df[c('customer_id', 'first_name', 'last_name', 'email', 'drug_company_1', 'drug_name_1', 'quantity_1', 'price_1', 'purchase_timestamp' )]
colnames(A)[colnames(A)=="drug_company_1"] <- "company_name"
colnames(A)[colnames(A)=="drug_name_1"] <- "drug_name"
colnames(A)[colnames(A)=="quantity_1"] <- "quantity"
colnames(A)[colnames(A)=="price_1"] <- "price"

B <-df[c('customer_id', 'first_name', 'last_name', 'email', 'drug_company_2', 'drug_name_2', 'quantity_2', 'price_2', 'purchase_timestamp')]
colnames(B)[colnames(B)=="drug_company_2"] <- "company_name"
colnames(B)[colnames(B)=="drug_name_2"] <- "drug_name"
colnames(B)[colnames(B)=="quantity_2"] <- "quantity"
colnames(B)[colnames(B)=="price_2"] <- "price"

AB<- rbind(A,B)

# Map company_id
company_id_list <- sapply(AB$company_name, function(x) temp_drug_comp_df$company_id [temp_drug_comp_df$company_name == x])
                        
# Add company_id to the main dataframe
AB$company_id <- company_id_list

# Map drug_id
drug_id_list <- sapply(AB$drug_name, function(x) temp_drug_name_df$drug_id [temp_drug_name_df$drug_name == x])
                        
# Add drug_id to the main dataframe
AB$drug_id <- drug_id_list

sapply(AB, class)
sapply(AB, mode)

AB$price <- as.numeric(gsub("\\$", "", AB$price))


purchases <-AB[c('customer_id', 'company_id', 'drug_id', 'quantity', 'price', 'purchase_timestamp')]
dbWriteTable(con, name="purchases_table", value=purchases, row.names=FALSE, append=TRUE)

########################################### populate drug_company_offerings_table

offerings<- AB[c('company_id', 'drug_id')]
offerings<-offerings[!duplicated(offerings[c('company_id', 'drug_id')]),]
offerings[duplicated(offerings[c('company_id', 'drug_id')]),]
dbWriteTable(con, name="drug_company_offerings_table", value=offerings, row.names=FALSE, append=TRUE)


##################

# -- END R CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 3 (2 points)
#  ---------------------
#  Write the R code that queries the "abc_pharmacy" database and returns
#  the customer name(s) and total purchase cost of the top 3 most expensive
#  transactions.
#
#  Type the actual result as part of your answer below, as a comment.
#

# -- START R CODE --

# Pass the SQL statement to filter data 
stmt <- "
WITH transactions_and_purchases (customer_id, first_name, last_name, purchase_timestamp, total) AS (
SELECT pt.customer_id, ct.first_name, ct.last_name, purchase_timestamp, quantity*price AS total
FROM purchases_table pt
JOIN customer_table ct 
ON ct.customer_id=pt.customer_id
)
SELECT first_name, last_name, SUM(total)as totals 
FROM transactions_and_purchases tp
GROUP BY tp.first_name, tp.last_name, tp.purchase_timestamp
ORDER BY totals DESC 
LIMIT 3
;
"

# Execute the statement and store results in a temporary dataframe
temp_df <- dbGetQuery(con, stmt)

# Show results
temp_df


# -- END R CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 4 (5 points)
#  ---------------------
#  Write the R code that queries the "abc_pharmacy" database and creates a
#  histogram of drug prices (price on the x-axis, frequency on the
#  y-axis). The figure must have proper axis titles, title and legend.
#
#  Result should be one figure, do not produce separate figures.
#

# -- START R CODE --

stmt <- "
SELECT price
FROM purchases_table;
"
# Execute the statement and store results in a temporary dataframe
temp_df2 <- dbGetQuery(con, stmt)

# Show results
temp_df2

hist (temp_df2$price, 
col=c("violet", "gray"), 
xlab="Price", 
las =1, 
main="Histogram of Drug Prices", breaks=60)


# -- END R CODE --

# -----------------------------------------------------------------------------
