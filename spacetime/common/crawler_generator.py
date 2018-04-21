def generate(app_id):
    uas, filename, typenames = generate_crawler_frame(app_id)
    generate_datamodel(app_id)
    return uas, filename, typenames

def generate_crawler_frame(app_id):
    useragentstring = "IR Crawler W18 {0}".format(app_id)
    filename = app_id
    typenames = ("{0}Link".format(app_id), 
                 "{0}UnprocessedLink".format(app_id), 
                 "One{0}UnProcessedLink".format(app_id), 
                 "get_downloaded_content", "add_server_copy")
    open("applications/search/crawler_frame.py", "w").write(
        open("applications/search/crawler_frame_template.txt").read().format(
            app_id, filename))
    open("applications/search/reset_frontier.py", "w").write(
        open("applications/search/reset_frontier_template.txt").read().format(
            app_id, filename))
    open("applications/search/check_frontier.py", "w").write(
        open("applications/search/check_frontier_template.txt").read().format(
            app_id, filename))
    open("applications/search/delete_invalids_from_frontier.py", "w").write(
        open("applications/search/delete_invalids_from_frontier_template.txt").read().format(
            app_id, filename))
    return useragentstring, filename, typenames

def generate_datamodel(app_id):
    useragentstring = "IR Crawler W18 {0}".format(app_id)
    filename = app_id
    typenames = ("{0}Link".format(app_id), 
                 "{0}UnprocessedLink".format(app_id), 
                 "One{0}UnProcessedLink".format(app_id), 
                 "get_downloaded_content", "add_server_copy")
    open("datamodel/search/{0}_datamodel.py".format(filename), "w").write(
        open("datamodel/search/client_datamodel_template.txt").read().format(
            app_id, filename))
    return useragentstring, filename, typenames