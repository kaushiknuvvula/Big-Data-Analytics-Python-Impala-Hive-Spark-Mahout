# Read in data

#setwd("C:/Users/Kaushik Nuvvula/Desktop/Harvesting_Big_Data/Project_2/Visualization/Data_2")

school_count <- read.csv("Trend_SchoolsPerYear.csv", header = TRUE, sep = ",")

# Count of schools acrsoss years
library(ggvis)

school_count %>% ggvis(~Year, ~School_Count) %>% layer_bars(fill:="#aab5dc") %>% 
  add_axis("x", title = "Year") %>%
  add_axis("x", orient = "top", ticks = 0, title = "Count of Schools Across Years",
           properties = axis_props(
             axis = list(stroke = "white"),
             labels = list(fontSize = 0)))

#Distribution of Earnings vs University Control
earnings_2011 <- read.csv("Earnings2013.csv", header = TRUE, sep = ",")
library(ggplot2)

ggplot() + 
  geom_density(data=earnings_2011, aes(x=X75earn, group=Control, fill=Control),alpha=0.5, adjust=2) + 
  xlab("75%ile Earnings") +
  ylab("") +labs(title="Distribution of Earnings based on University Control")

library(xlsx)
cor_a <- read.xlsx("Sat_adm.xlsx",1,header = TRUE)
library(ggplot2)

g2<-ggplot(cor_a) + geom_line(aes(x=Sat_Avg, y=adm_rate), stat='identity')
#lets make the axis labels better
g2<-g2+labs(title="SAT Average vs Admission Rate", x="SAt AVerage", y="Admission Rate")
g2+ theme(axis.text.x = element_text(angle = 90, hjust = 1))


control <- read.csv("grade_rate.csv", header = TRUE, sep = ",")

control = as.data.frame.matrix(control) 

library(reshape2)
control1 <- melt(control, id.var="Year")

library(ggplot2)
ggplot(control1, aes(x = Year, y = value, fill = variable)) + 
  geom_bar(stat = "identity")



#Distribution of Earnings vs University Control
#trend <- read.csv("trend_control.csv", header = TRUE, sep = ",")
#library(ggplot2)

#ggplot() + 
#  geom_histogram(data=trend, aes(x=Year, group=Count_1, fill=Control),alpha=0.5, adjust=2) + 
#  xlab("Year") +
#  ylab("") +labs(title="Distribution of Earnings based on University Control")








