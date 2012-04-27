var restify = require("restify"),
    cutils = require("./cloud_utils.js"),
    vm = require("vm"),
    util = require("util");

exports.start = function(port){
    var server = restify.createServer();
    server.use(restify.bodyParser({"mapParams": false}));

    server.get("/ping", function(req, res, next){
        res.send("pong");
        next();
    });

    server.post("/submit/", function(req, res, next){
        if(!req.body.job){
            res.json({"error": "No job."});
        }
        console.log(req.body.job);
        var job = JSON.parse(new Buffer(decodeURIComponent(req.body.job), "base64").toString());
        var job_id = req.body.job_id;
        var fragment_id = req.body.fragment_id;
        var res_port = req.body.response_port;
        res.json(200, {"job_id": job_id});
        // exec the job
        var obj = execJob(job);
        // when done, POST the results to http://controller/result_endpoint/
        var remote_host = req.socket.remoteAddress + ":" + res_port;
        var client = restify.createStringClient({"url": "http://" + remote_host});
        console.log("posting result to " + "http://" + remote_host);
        client.post("/submit_result/", 
                    {
                        "result": cutils.buEncode(obj),
                        "job_id": job_id,
                        "fragment_id": fragment_id,
                        "worker_port": port
                    },
                    function(err, req, res, data){
                        console.log(data);

                    });
    });

    server.listen(port, function(){
        console.log("listening on port %d", port);
    });
};

function execJob(job){
    console.dir(job);
    var retname = "ret";
    while(job.ctx.hasOwnProperty(retname))
        retname = '_' + retname;
    job.ctx[retname] = null;
    var the_script = retname + " = (" + job.func + ")(" + job.args.map(JSON.stringify) + ");";
    vm.runInNewContext(the_script, job.ctx);
    var ret = job.ctx[retname];
    delete job.ctx[retname];

    return {"ctx": job.ctx,
            "ret": ret};
}
