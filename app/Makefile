BASE="jupyterhub.cirrus.carlboettiger.info"
MAKEFLAGS += s

.PHONY: serve
serve: 
	@echo "\n 🌎  preview at: \033[1m https://${BASE}${JUPYTERHUB_SERVICE_PREFIX}proxy/8501/ \033[0m \n"
	streamlit run app.py --server.port 8501  1> /dev/null 2>&1

