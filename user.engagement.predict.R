# initial setup ------
require(RJDBC, quiet = T)
require(glmnet, quiet = T)
require(pROC, quiet = T)

query.db <- function(conn, query, n = 0) {
  out <- data.frame()
  
  res <- dbSendQuery(conn, query)
  if (n == 0) {
    out <- dbFetch(res)
  } else {
    while (1) {
      rows <- fetch(res, n)
      if (nrow(rows) == 0)
        break
      
      out <- rbind(out, rows)
    }
  }
  
  dbClearResult(res)
  
  out
}

# change these values accordingly ----
# set it to FALSE if you've run this script once and data has been saved already
refresh.data <- T  
# app id of your app in mParticle platform
appid = 191  
# appPlatformId's you'd like to look at, comma separated if multiple are listed
appplatformid <- "1202, 1203"  
# name of the app
app.name <- "Demo app"  
# length of time period after install, in ms
time.length <- 2 * 3600000 
# revenue threshold for identifying high value users
revenue.threshold <- 0.01  # 1 cent
# number of active days for identifying medium value users
active.days <- 5
# redshift server
server <- "org1234.abcdefg.us-east-1.redshift.amazonaws.com"
# redshift port number
port <- 5439
# redshift database name
database.name <- "test"
# user name and password used to connect to redshift
user.name <- "dev"
password <- "yourpassword"
# path to the jdbc driver file on your file system
driver.path <- "C:/Work/postgresql-8.4-703.jdbc4.jar"

events.table <- paste("app_", appid, ".eventsview", sep = '')

tryCatch({
# get data ----
  if (refresh.data) {
    drv <- JDBC("org.postgresql.Driver", driver.path)
    
    conn <- dbConnect(drv, paste("jdbc:postgresql://", server, ":", port, "/", database.name, sep = ''), user.name, password)
    
    # step 1: find users who installed the app
    query <- paste("create temp table installedusers as
    select mparticleuserid, min(eventtimestamp) as installtime
    from ", events.table, "
    where appplatformid in (", appplatformid, ") and isdebug = 0 and eventdate between current_date - 40 and current_date - 31  -- 10 days of app installs are collected
    and messagetypeid = 7
    group by 1;", sep = '')
    
    dbSendUpdate(conn, query)
    
    if (query.db(conn, "select count(*) from installedusers") == 0)
      stop("there're no installed users")
    
    # step 2: find high valued users
    query <- paste("
      create temp table highvalueusers as
      select mparticleuserid, sum(eventltvvalue) as ltv
      from ", events.table, "
      join installedusers using (mparticleuserid)
      where appplatformid in (", appplatformid, ") and isdebug = 0 
      and eventtimestamp > installtime + ", time.length, "
      and eventltvvalue >= ", revenue.threshold, "
      group by 1", sep = '')
    
    dbSendUpdate(conn, query)
    
    # step 3: find medium valued users
    query <- paste("
    create temp table mediumvalueusers as
    select mparticleuserid, sum(eventltvvalue) as ltv
    from ", events.table, "
    join installedusers using (mparticleuserid)
    where appplatformid in (", appplatformid, ") and isdebug = 0
    and eventtimestamp > installtime + ", time.length, "
    and mparticleuserid not in (select mparticleuserid from highvalueusers)
    group by 1
    having count(distinct eventdate) > ", active.days, sep = '')
    
    dbSendUpdate(conn, query)
    
    # step 4: get distinct custom app event names, each of which will be used in user features
    query <- paste("select eventname
      from ", events.table, "
      join installedusers using(mparticleuserid)
      where appplatformid in (", appplatformid, ") and isdebug = 0 
      and eventtimestamp between installtime and installtime + ", time.length, "
      and messagetypeid = 4
      group by 1
      having approximate count(distinct mparticleuserid) > 10", sep = '')
    
    event.names <- unlist(query.db(conn, query))
    event.names <- gsub("'", "\\\\\'", event.names)
    transformed.event.names <- tolower(sapply(event.names, gsub, pattern = "[^[:alnum:]]", replacement = ""))
    
    # step 5: calculate user features
    query <- "
    create temp table userfeatures as
    select mparticleuserid,
    isnull(sum(case when messagetypeid = 1 then 1 end), 0) as sessions,
    avg(case when messagetypeid = 2 and eventlength is not null then eventlength / 1000.0 end) as avgsessionlength,
    sum(case when messagetypeid = 2 and eventlength is not null then eventlength / 1000.0 end) as totaltimeinapp,
    isnull(sum(case when messagetypeid = 3 then 1 end), 0) as screenviews,
    isnull(sum(case when messagetypeid = 4 then 1 end), 0) as appevents,
    isnull(sum(case when messagetypeid = 5 and exceptionhandled is true then 1 end), 0) as exceptions,
    isnull(sum(case when messagetypeid = 5 and exceptionhandled is not true then 1 end), 0) as crashes,
    isnull(sum(case when messagetypeid = 10 then 1 end), 0) as asts,
    isnull(sum(case when messagetypeid = 11 then 1 end), 0) as pushes,
    isnull(sum(eventltvvalue), 0) as revenue"
    
    for (i in 1 : length(event.names)) {
      query <- paste(query, ",\nisnull(sum(case when messagetypeid = 4 and eventname = '", event.names[i], "' then 1 end), 0) as event_", transformed.event.names[i], sep = '')
    }
    
    query <- paste(query, "
    from ", events.table, "
    join installedusers using(mparticleuserid)
    where appplatformid in (", appplatformid, ") and isdebug = 0 and  eventtimestamp between installtime and installtime + ", time.length, "
    group by 1;", sep = '')
    
    dbSendUpdate(conn, query)
    
    # step 6: format user data for modeling
    query <- "
    create temp table usersdata as
    select a.*, 
      case when b.mparticleuserid is null then 
        case when c.mparticleuserid is null then 0 else 1 end 
      else 2 end as label
    from userfeatures a
    left join highvalueusers b using(mparticleuserid)
    left join mediumvalueusers c using(mparticleuserid);"
    
    dbSendUpdate(conn, query)
    
    # step 7: read data into memory
    data <- query.db(conn, "select * from usersdata", n = 10000)
    
    # save modeling data into a file for future use
    save(list = c("data"), file = paste(app.name, ".install.to.engagement.data", sep = ''))
  } else {
    load(paste(app.name, ".install.to.engagement.data", sep = ''))
  }

# model building ----
  # just keep low and high value users
  if (sum(data$label == 2) > 0) {
    data$label[data$label == 1] <- 0
    data$label[data$label == 2] <- 1
  }
  
  n.rows <- nrow(data)
  train.ind <- sample(1 : n.rows, 0.8 * n.rows)
  train <- data[train.ind, -c(1)]
  test <- data[-train.ind, -c(1)]
  
  # remove NA values
  train$avgsessionlength[is.na(train$avgsessionlength)] <- 0
  train$totaltimeinapp[is.na(train$totaltimeinapp)] <- 0
  test$avgsessionlength[is.na(test$avgsessionlength)] <- 0
  test$totaltimeinapp[is.na(test$totaltimeinapp)] <- 0
  
  print(dim(train))
  
  fit <- cv.glmnet(x = as.matrix(subset(train, select = -c(label))), y = as.factor(train$label), family = "binomial")
  
  pred <- predict(fit, newx = as.matrix(subset(test, select = -c(label))), s = "lambda.1se", type = "response")[, 1]

  auc.stats <- c(roc(test$label, pred)$auc)
  
  print(paste(app.name, auc.stats))
  
  # find the coeffients
  coeff <- fit$glmnet.fit$beta[, which(fit$lambda == fit$lambda.1se)]
  
  # investigate the most important variables
  print(coeff[order(abs(coeff))])
  
}, error = function(e) {
  print(e)
})

