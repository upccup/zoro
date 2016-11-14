package api

import "github.com/gin-gonic/gin"

func (api *Api) ApiRouter() *gin.Engine {
	router := gin.New()

	v1Router := router.Group("/v1")
	{
		v1Router.GET("/raft/status", api.RaftStatus)
		v1Router.GET("/raft/leader", api.RaftLeadr)
	}

	return router
}
