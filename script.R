
library(Hmisc)
library(gtools)

stopifnot(require(Rgraphviz))
library(igraph)
library(farff)
library(pcalg)
mpath = 'D:/Ry/mozil/PyGit - DsData 1 3 2018/cmp/blamed/'
savepath = "D:/GISOUT/GC2/"
setwd(mpath)

files = list.files(path = ".")


framesLst = list()


i = 1
for(file in files)
{

  framesLst[[i]] <- readARFF(paste(mpath,file,sep=''))
  i=i+1

}

countFiles = length(files)

for(file in files[c(1,48,47,27)])
{
  instBlamed = NULL
  instBlamed <- data.frame()
  rmind = which(files == file)
  print (rmind)
  inds = seq (1:countFiles)[-rmind]
  for(ind in inds)  
  {
    instBlamed <- rbind(instBlamed, framesLst[[ind]])
  }
  
  iBlamed <- Filter(function(x)(length(unique(x))>1), instBlamed)
  
  iBlamed[,1:24] <- scale(iBlamed[,1:24])  
  print (file)
  print (dim(iBlamed))
  
  suffStat.Blamed <- list(C = cor(iBlamed), n = nrow(iBlamed))
  pc.Blamed <- pc(suffStat.Blamed, indepTest =  gaussCItest, alpha = 0.05, labels = colnames(iBlamed), skel.method = "stable")
  
  res.Blamed <- pcSelect(iBlamed[,25], iBlamed[,-25],  corMethod = "standard", alpha = 0.05)
  
  res.Blamed$G[which(res.Blamed$G == T)]  
  res.Blamed$zMin[which(res.Blamed$G == T)]
  
  pdf(paste(savepath,file,".pdf", sep=""))
  
  plot(pc.Blamed , zvalue.lwd=T, directed = T, main = paste("Markov Equivalence Class for Blamed: ",file," , alpha = 0.05"))
  
  dev.off()
  
  graphics.off()
  
}


