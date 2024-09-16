data <- read.delim('clipboard')

library("dtwclust")
require("TSclust")


require("doParallel")
# Create parallel workers
workers <- makeCluster(3L)
# Preload dtwclust in each worker; not necessary but useful
invisible(clusterEvalQ(workers, library(#"dtwclust",
                                          "TSclust")))
# Register the backend; this step MUST be done
registerDoParallel(workers)

# Stop parallel workers
# stopCluster(workers)
# Go back to sequential computation
# registerDoSEQ()


options(scipen = 9999,
        max.print = 9999)
if (!require("pacman")) install.packages("pacman")
pacman::p_load(jpeg, png, ggplot2, lubridate, readxl, reshape2, tibbletime, dplyr, extrafont, 
               ggthemes, tidyverse, gridExtra, grid, gtable, ISOweek, ggpubr, ggrepel)

date_in_week <- function(year, week, weekday){
  w <- paste0(year, "-W", sprintf("%02d", week), "-", weekday)
  ISOweek2date(w)
}

dat <- data

# secuencia <- data.frame(Año = c(rep(2018,53), rep(2019,53),rep(2020,53),rep(2021,48)),
#                         Semana = c(rep(seq(1:53),3),1:48))

# dat <- secuencia %>% 
#   dplyr::left_join(dat, c("Año","Semana"))

dat$Inicio <- date_in_week(year = dat$Año, week = as.numeric(dat$Semana), weekday = 1)
dat$Fin <- date_in_week(year = dat$Año, week = as.numeric(dat$Semana), weekday = 7)
dat$Date <- as.Date(dat$Fin, format = "%m/%d/%y")
dat <- as_tbl_time(dat, index = Date)%>%
  filter(Date >= as.Date("2018-01-01"))

# expand <- expand.grid(Date = unique(dat$Date), parcela = unique(paste(na.omit(dat$M),
#                                                                       "_._",na.omit(dat$Turno),
#                                                                       "_._",na.omit(dat$válvula)))) %>%
#   as.data.frame()
# 
# expand <- distinct(expand)
# 
# sum(duplicated(expand))
# 
# library(stringr)
# 
# parcela <- strsplit(as.character(expand$parcela),'_._') %>%as.data.frame()%>% 
#   t()
# 
# colnames(parcela) <- c("M","Turno","válvula")
# row.names(parcela) <- NULL
# 
# expand_f <- data.frame(Date = as.Date(expand$Date), parcela) %>%
#   mutate(M = as.numeric(M),
#          Turno = as.numeric(Turno),
#          válvula = as.numeric(válvula))
# 
# dat <- expand_f %>% 
#   dplyr::left_join(dat, c("Date","M","Turno","válvula"))

library(tidyr)
library(reshape)

a <- 0
b <- 1

M1 <- dat %>%
  #dplyr::filter(M %in% c("1")) %>%
  # &
  # Turno %in% c("4")
  # dplyr::filter(!(M %in% c("1") &
  #          Turno %in% c("1") &
  #          válvula %in% c("4")|
  #            M %in% c("3") &
  #            Turno %in% c("3") &
  #            válvula %in% c("6"))) %>%
  dplyr::group_by(M,Turno,#válvula,#Variedad,
           Date)%>%
  dplyr::summarise(Kg_Total = sum(Kg_Total,na.rm = T),
            Área = sum(Área,na.rm = T)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(Kg_Ha = Kg_Total/Área) %>%
  dplyr::mutate_at(c(6), ~replace(., is.infinite(.), 0)) %>%
  dplyr::mutate(Kg_Ha = log(Kg_Ha+1)) %>%
  dplyr::mutate(Kg_Ha = (Kg_Ha-min(Kg_Ha))/(max(Kg_Ha)-min(Kg_Ha))*(b-a)+a) %>%
  dplyr::select(Date, M,Turno, #válvula, #Variedad,
         Kg_Ha) %>%
  tidyr::pivot_wider(names_from = c("M","Turno",#"válvula",#"Variedad"
                               ),
              values_from = "Kg_Ha")%>%
  dplyr::mutate(Date = as.Date(Date)) %>%
  dplyr::mutate_at(c(-1), ~replace(., is.na(.), 0)) %>%
  dplyr::arrange(Date) %>%
  # t() %>%
  as.list()%>%
  as.data.frame()%>%
  # dplyr::select(-X1_1_2_SPRINGHIGH,-X1_1_3_SNOWCHASER,
  #        -X1_1_5_LEGACI,-X1_1_5_VICTORIA.,
  #        -X1_3_5_EMERALD,-X1_3_5_SNOWCHASER,
  #        -X1_3_5_VENTURA,#-X6_1_7_BILOXI,
  #        #-X6_4_7_BILOXI
  #        )%>%
  as.list()

print(colSums(M1%>%as.data.frame()%>%select(-1))[colSums(M1%>%as.data.frame()%>%
                                                           select(-1)) %in% c(0)])

length(names(M1[-1L]))

M1[["Date"]]

M1 <- as_tbl_time(M1%>%as.data.frame(), index = Date)%>%
  filter(Date >= as.Date("2021-10-17")) %>% as.list()

# Normalized DTW
ndtw <- function(x, y, ...) {
  dtw(x, y, ...,
      step.pattern = asymmetric,
      distance.only = TRUE)$normalizedDistance
}
# Register the distance with proxy
proxy::pr_DB$set_entry(FUN = ndtw, names = c("nDTW"),
                       loop = TRUE, type = "metric", distance = TRUE,
                       description = "Normalized, asymmetric DTW")
# Partitional clustering
ts_clust <- tsclust(M1[2L:17L], k =3L,
        distance = "nDTW", seed = 838)

plot(ts_clust)
plot(ts_clust, type = "series")
plot(ts_clust, type = "centroids")

# Focusing on the first cluster
plot(ts_clust, type = "series", clus = 1L)
plot(ts_clust, type = "centroids", clus = 1L)

data <- zscore(M1[2L:16L])

pc_dtw <- tsclust(M1[-1L], k = 5L,
                  distance = "dtw_basic", centroid = "dba",
                  trace = TRUE, seed = 8,
                  norm = "L2", window.size = 20L,
                  args = tsclust_args(cent = list(trace = TRUE)))

plot(pc_dtw)
pc_dtw@cluster



# The series and the obtained prototypes can be plotted too
plot(pc_dtw, type = "sc")
plot(pc_dtw, type = "centroids")


# Reinterpolate to same length
data <- reinterpolate(M1[2L:21L], new.length = max(lengths(M1)))

# z-normalization
data <- zscore(data[1L:45L])

pc_dtw <- tsclust(M1[2L:44L], k = 4L,
                  distance = "dtw_basic", centroid = "dba",
                  trace = TRUE, seed = 8,
                  norm = "L2", window.size = 20L,
                  args = tsclust_args(cent = list(trace = TRUE)))

pc_dtwlb <- tsclust(M1[2L:44L], k = 4L,
                    distance = "dtw_lb", centroid = "dba",
                    trace = TRUE, seed = 8,
                    norm = "L2", window.size = 20L,
                    control = partitional_control(pam.precompute = FALSE),
                    args = tsclust_args(cent = list(trace = TRUE)))

pc_ks <- tsclust(M1[2L:44L], k = 4L,
                 distance = "sbd", centroid = "shape",
                 seed = 8, trace = TRUE)

pc_tp <- tsclust(M1[2L:44L], k = 4L, type = "t",
                 seed = 8, trace = TRUE,
                 control = tadpole_control(dc = 2.5,
                                           window.size = 20L))


sapply(list(DTW = pc_dtw, DTW_LB = pc_dtwlb, kShape = pc_ks, TADPole = pc_tp),
       cvi, b = names(M1[-1L]), type = "VI")


acf_fun <- function(series, ...) {
  lapply(series, function(x) {
    as.numeric(acf(x, lag.max = 50, plot = FALSE)$acf)
  })
}
# Fuzzy c-means
fc <- tsclust(M1[2L:20L], type = "f", k = 2L,
              preproc = acf_fun, distance = "L2",
              seed = 42)
# Fuzzy membership matrix
fc@fcluster

all.equal(rep(1, 19), rowSums(fc@fcluster), check.attributes = FALSE)

# Plot crisp partition in the original space
plot(fc, series = M1[-1L], type = "series")


# Cluster evaluation 
fc <- tsclust(M1[-1L], type = "f", k = 2L:4L,
              preproc = acf_fun, distance = "L2",
              seed = 42)

names(fc) <- paste0("k_", 2L:4L)
sapply(fc, cvi, type = "internal")

fc <- tsclust(M1[-1L], type = "f", k = 2L,
              preproc = acf_fun, distance = "L2",
              seed = 42)

print(data.frame(names(M1[-1L]),fc@cluster),row.names = F)
plot(fc, series = M1[-1L], type = "series")


pc_k <- tsclust(M1[-1L], type = "p", k = 2L:50L,
                distance = "dtw_basic", centroid = "mean",
                seed = 94L)
names(pc_k) <- paste0("k_", 2L:50L)
sapply(pc_k, cvi, type = "internal")
max(sapply(pc_k, cvi, type = "internal")["D",])
#max(sapply(pc_k, cvi, type = "internal")["Sil",])
min(sapply(pc_k, cvi, type = "internal")["DB",])
min(sapply(pc_k, cvi, type = "internal")["DBstar",])

pc <- tsclust(M1[-1L], type = "p", k = 2L,
                distance = "dtw_basic", centroid = "mean",
                seed = 94L)

print(data.frame(names(M1[-1L]),pc@cluster),row.names = F)
plot(pc, series = M1[-1L])

plot(M1[[20]],type = "l")

library(fpc)
hclusters <- clusterboot(data = M1[-1L]%>%as.data.frame(),
                         B = 100, 
                         clustermethod = dbscanCBI,
                         # k = 3,
                         eps = 0.2,
                         #method = "dist",
                         MinPts = 1,
                         seed = 123)

# Los promedios deben salir lo mÃ¡s cercano a 1 posible.
# Un valor > 0.75 Ã³ 0.85 es muy bueno.
# Un valor < 0.6 es malo
hclusters$bootmean

set.seed(20000)
options(digits=3)
dface <- dist(M1[-1L]%>%as.data.frame()%>%as.matrix())
complete <- cutree(hclust(dface),c(8))
clustatsum(datadist = dist(M1[-1L]%>%as.data.frame()%>%as.matrix()),
           datanp=M1[-1L]%>%as.data.frame()%>%
             as.matrix(),complete,npstats = T)%>%t()
