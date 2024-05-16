# Indian Premier League Data Analysis 

Ball By Ball Data of all the IPL seasons (637 matches including 2017).

## SUMMARY
This data set has the ball by ball data of all the Indian Premier League (IPL) matches till 2017 season. This is made up of six different datasets.

Source: http://cricsheet.org/ (data is available on this website in the YAML format. This is converted to CSV format by using R Script ,SQL,SSIS.

Project Design : https://www.youtube.com/watch?v=0iNJPKheQqM 


### Description
In this project I will be using Apache Spark, Python, & SQL to complete this project.

The Indian Premier League is a cricket league. This data is made up of six seperate datasets.
- Ball by ball
- Match
- Player
- Player match
- Team
  
 The data dictionary for these datasets can be found here. https://data.world/raghu543/ipl-data-till-2017/workspace/data-dictionary

#### AWS S3 Bucket
The information for this project is stored within rhea-github AWS S3 bucket.

#### Databricks platform
Databricks platform and juypter notebook was utilized for this project.


### Visualizations

#### Top 10 Economical Players within Indian Premier League 

This shows the top ten economical players. An economical bowler/player is one who concedes relatively few runs per over while bowling.

![image](https://github.com/rhejos/ipl_data_analysis/assets/153791988/bd85fb2d-4401-422e-a3c0-4c594d9eca98)

#### Impact of winning toss on match outcomes

This shows the count of matches that were won or loss if the team won the coin toss.

![image](https://github.com/rhejos/ipl_data_analysis/assets/153791988/c74f90fe-3b9c-45c1-aa89-ef862ce69616)

#### The Top 10 scorers avergae runs for winning matches 

This goes over the average runs the top scorer had for winning matches.

![image](https://github.com/rhejos/ipl_data_analysis/assets/153791988/41ba4093-bb67-4593-b101-72a2ccca91fe)

#### Distribution of Scores by Venue 

This explores the coring trends based on match venues.

![image](https://github.com/rhejos/ipl_data_analysis/assets/153791988/bb20a495-ec62-4c1e-86e4-8caf808bc82c)


#### Team performance

This ranks teams performance by how many wins they had after winning the toss.

![image](https://github.com/rhejos/ipl_data_analysis/assets/153791988/19e53869-6b4c-465f-874f-ecbb33c943d7)


