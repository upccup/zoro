package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (api *Api) RaftStatus(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"code": 0, "data": api.Node.Status()})
	return
}

func (api *Api) RaftLeadr(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"code": 0, "data": api.Node.Status().Lead})
	return
}

func (api *Api) SaveRaftData(ctx *gin.Context) {
	err := api.Node.ProposeValue(ctx, nil, nil)
	ctx.JSON(http.StatusOK, gin.H{"code": 0, "data": err.Error()})
	return
}

func (api *Api) GetRaftData(ctx *gin.Context) {
	key := ctx.Param("key")
	val, err := api.Store.GetKeyValue(key)

	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 0, "data": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"code": 0, "data": val})
	return
}
