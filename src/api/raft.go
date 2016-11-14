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
