/* worker.js
 * The worker module for cloud.js
 * This is where the a function is actually executed. The interface is RESTful,
 * exposing the following endpoints:
 * /ping/
 *      - A GET at the above endpoint returns a text/plain response of the only
 * string "pong", sans the quotes.
 *
 * /submit/
 *      - The endpoint used to submit a function, a set of arguments and the
 * execution context via HTTP POST. The mandatory parameters are:
 *
 *  "job" - The job object's string dump, properly encoded(read on) 
 *  "job_id" - self explanatory. passed back in the response.
 *  "fragment_id" - passed back in the response as is.
 *  "response_port" - the port number to POST the response on the caller.
 *
 * Job structure:
 * The job object looks like this
 *          
 *          {
 *              "func": "function(a1, a2....,aN){...}",
 *              "args": [arg1, arg2, arg3..., argN],
 *              "ctx": {
 *                  "c1": v1,
 *                  "c2": v1
 *
 *                   ...
 *
 *                  "cM": vM    
 *              }
 *          }
 *
 * Note that the function body is a string object, as functions themselves are 
 * not JSON seriazable.
 *
 * The args property is an array of arguments, which will be passed in order as
 * positional arguments to the given function, as if calling func.apply(null, args)
 *
 * The ctx property holds any variables/values the function accesses in its body
 * but has not defined them - i.e., the context provides a "sandbox" to simulate
 * an enclosing scope for the function
 *
 * Before POSTing to the /submit/ endpoint, the submitter must ensure that the 
 * job object is converted to _string_ form AND Base64 encoded AND THEN URL 
 * encoded. This is straightforward with Node.js
 *
 *      job_object = {...}
 *      encoded = encodeURIComponent(new Buffer(JSON.stringify(job_obj)).toString('base64'))
 *
 * See buEncode() in cloud_utils.js
 * The job is executed, and when the result is available, it is POSTed back to the 
 * caller. The POST parameter `response_port' is needed here.
 */
//------------------------------------------------------------------------------
var restify = require("restify"),
    cutils = require("./cloud_utils.js"),
    vm = require("vm"),
    util = require("util");
//------------------------------------------------------------------------------
// start(port:integer)
// spawn a worker and listen for connections on the specified port.
//------------------------------------------------------------------------------
exports.start = function(port){
    var server = restify.createServer();
    server.use(restify.bodyParser({"mapParams": false}));

    server.get("/ping", function(req, res, next){
        res.send("pong");
        next();
    });
    // When we have a job,...
    server.post("/submit/", function(req, res, next){
        if(!req.body.job){
            res.json({"error": "No job."});
        }
        console.log(req.body.job);
        // ...decode it...
        var job = JSON.parse(new Buffer(decodeURIComponent(req.body.job), "base64").toString());
        var job_id = req.body.job_id;
        var fragment_id = req.body.fragment_id;
        var res_port = req.body.response_port;
        res.json(200, {"job_id": job_id});
        // ... and exec it 
        var obj;
        try{
            obj = execJob(job);
        }catch(e){
            obj = {"error": e.toString()};
        }
        // when done, POST the results to http://controller/submit_result/
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
//------------------------------------------------------------------------------
// execJob(job:object)
// Excute the job using the Node.js `vm' module. It basically wraps the job 
// function and generates a suitable name for the return variable and then
// passes the follwing script to vm:
//      ret = (function(){...})(arg1, arg2...argN);
// if the variable name `ret' is used already in the passed context, _ret is used
// else __ret and so on.
// The trick here is that we add this generated variable name to the context
// passing the script to the vm.runInNewContext() function, so that the _script_
// now has access to it. Then while returning to the caller, we remove this 
// property from the context.
//------------------------------------------------------------------------------
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
//------------------------------------------------------------------------------