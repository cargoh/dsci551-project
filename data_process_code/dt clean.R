library(stringr)

setwd("D:\\Self\\School\\USC\\DSCI551\\Final Project\\")
NYClist <- read.csv('D:\\Self\\School\\USC\\DSCI551\\Final Project\\NYC listings.csv'
                    ,header = TRUE
                    ,stringsAsFactors = FALSE)

NYClist_d <- read.csv('D:\\Self\\School\\USC\\DSCI551\\Final Project\\NYC listings DETAIL.csv'
                    ,header = TRUE
                    ,stringsAsFactors = FALSE)

Bostonlist_d <- read.csv('D:\\Self\\School\\USC\\DSCI551\\Final Project\\Boston listings DETAIL.csv'
                      ,header = TRUE
                      ,stringsAsFactors = FALSE)

delNonASCII <- function(dt,colnmlst){
  for (i in (1:nrow(dt))){
    for (colnm in colnmlst){
      dt[[colnm]][i] <- iconv(dt[[colnm]][i], "latin1", "ASCII", sub="")
    } 
  }
  return(dt)
}

costr <- function(dt,target_col,col_a,col_b){
  for (i in (1:nrow(dt))){
    a <- dt[[col_a]][i]
    b <- dt[[col_b]][i]
    dt[[target_col]][i] <- paste(a,b,sep = ', ')
  }
  return(dt[[target_col]])
}




NYClist_d$id <- as.numeric(NYClist_d$id)
NYClist_d$host_id <- as.numeric(NYClist_d$host_id)

NYClist_d <- na.omit(NYClist_d)

NYClist_d <- delNonASCII(NYClist_d,c("description",'name','host_about','neighborhood_overview'))

NYClist_d[['neighbourhood']] <- costr(NYClist_d,"neighbourhood","neighbourhood_group_cleansed","neighbourhood_cleansed")

NYClist_d$price <- str_replace(NYClist_d$price, '\\$', '')

NYClist_d[which(NYClist_d$availability_365 == 0),][['has_availability']] <- 'f'

NYClist_d <- NYClist_d[ , -which(names(NYClist_d) %in% c('listing_url','scrape_id','host_url','host_thumbnail_url'
                                                         ,'host_picture_url','host_neighbourhood','host_total_listings_count'
                                                         ,'property_type'
                                                         ,"minimum_minimum_nights","maximum_minimum_nights"
                                                         ,"minimum_maximum_nights","maximum_maximum_nights"                     
                                                         ,"minimum_nights_avg_ntm","maximum_nights_avg_ntm"
                                                         ,"availability_30","availability_60","availability_90"
                                                         ,"calendar_last_scraped","number_of_reviews_ltm","number_of_reviews_l30d"
                                                         ,"license","neighbourhood_group_cleansed","neighbourhood_cleansed"
                                                         ,"calculated_host_listings_count","calculated_host_listings_count_entire_homes"
                                                         ,"calculated_host_listings_count_private_rooms"
                                                         ,"calculated_host_listings_count_shared_rooms","neighborhood_overview"
                                                         ,"picture_url"
                                                         ,"review_scores_accuracy","review_scores_cleanliness","review_scores_checkin"                       
                                                         ,"review_scores_communication","review_scores_location"                      
                                                         ,"review_scores_value","bathrooms"))]


Bostonlist_d$id <- as.numeric(Bostonlist_d$id)
Bostonlist_d$host_id <- as.numeric(Bostonlist_d$host_id)

Bostonlist_d <- na.omit(Bostonlist_d)

Bostonlist_d <- delNonASCII(Bostonlist_d,c("description",'name','host_about','neighborhood_overview'))

Bostonlist_d[['neighbourhood']] <- Bostonlist_d[["neighbourhood_cleansed"]]

Bostonlist_d$price <- str_replace(Bostonlist_d$price, '\\$', '')

Bostonlist_d[which(Bostonlist_d$availability_365 == 0),][['has_availability']] <- 'f'

Bostonlist_d <- Bostonlist_d[ , -which(names(Bostonlist_d) %in% c('listing_url','scrape_id','host_url','host_thumbnail_url'
                                                         ,'host_picture_url','host_neighbourhood','host_total_listings_count'
                                                         ,'property_type'
                                                         ,"minimum_minimum_nights","maximum_minimum_nights"
                                                         ,"minimum_maximum_nights","maximum_maximum_nights"                     
                                                         ,"minimum_nights_avg_ntm","maximum_nights_avg_ntm"
                                                         ,"availability_30","availability_60","availability_90"
                                                         ,"calendar_last_scraped","number_of_reviews_ltm","number_of_reviews_l30d"
                                                         ,"license","neighbourhood_group_cleansed","neighbourhood_cleansed"
                                                         ,"calculated_host_listings_count","calculated_host_listings_count_entire_homes"
                                                         ,"calculated_host_listings_count_private_rooms"
                                                         ,"calculated_host_listings_count_shared_rooms","neighborhood_overview"
                                                         ,"picture_url"
                                                         ,"review_scores_accuracy","review_scores_cleanliness","review_scores_checkin"                       
                                                         ,"review_scores_communication","review_scores_location"                      
                                                         ,"review_scores_value","bathrooms"))]


write.csv(NYClist_d,file="NYC_FinalList.csv",row.names=F)
write.csv(Bostonlist_d,file="Boston_FinalList.csv",row.names=F)

