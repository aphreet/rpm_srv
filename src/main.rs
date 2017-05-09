extern crate hyper;
#[macro_use]
extern crate log;
extern crate env_logger;

use hyper::method;
use hyper::server;
use hyper::status;
use std::fs;
use std::io;
use std::error::Error;
use std::path;
use std::string;

#[derive(Debug)]
pub struct HttpError {
    code: status::StatusCode,
    error: String
}

#[derive(Debug)]
pub struct RepoRequest {
    repo_name: String,
    file_name: Option<String>
}

impl RepoRequest {

    fn ensure_dir_exists(&self, path: &path::Path){
        if(!path.exists()){
            fs::DirBuilder::new()
                .recursive(true)
                .create(path);
        }

        if(!path.is_dir()){
            panic!("Path {:?} must refer to the directory", path);
        }
    }

    fn ensure_repo_exists(&self, root: &String) {
        let repo_path = path::Path::new(root).join(path::Path::new(&self.repo_name));
        self.ensure_dir_exists(&repo_path);
        let rpm_path = repo_path.as_path().join(path::Path::new("rpms"));
        self.ensure_dir_exists(rpm_path.as_path());
    }

    fn repo_path(&self, root: &String) -> String {
        let repo_path = path::Path::new(root).join(path::Path::new(&self.repo_name));
        repo_path.to_str().unwrap().to_owned()
    }

    fn file_path(&self, root: &String) -> String {
        let repo_path = self.repo_path(root);
        let rpm_path = path::Path::new(&repo_path).join(path::Path::new("rpms"));
        let name = self.file_name.to_owned().unwrap();
        let file_path = rpm_path.as_path().join(path::Path::new(&name));

        let extension = file_path.extension().unwrap().to_str().unwrap().to_owned();

        if extension != "rpm" {
            panic!("Unexpected file name {}, it must be rpm file", name);
        }

        file_path.to_str().unwrap().to_owned()
    }
}

fn drop_non_string_comp(c: &path::Component) -> bool {
    match *c {
        path::Component::Normal(_) => {true}
        _ => {false}
    }
}

fn convert_string_com(c: &path::Component) -> String {
    match *c {
        path::Component::Normal(val) => {val.to_str().unwrap().to_owned()}
        _ => panic!("Failed to parse component")
    }
}

fn parse_request(uri: &hyper::uri::RequestUri) -> Result<RepoRequest, HttpError> {
    match *uri {
        hyper::uri::RequestUri::AbsolutePath(ref val) => {
            let path = path::Path::new(val);

            let components:Vec<String> = path.components().filter(drop_non_string_comp).map(|c| convert_string_com(&c)).collect();
            match components.len() {
                1 => {Ok(RepoRequest{repo_name: components[0].to_owned(), file_name: None})}
                2 => {Ok(RepoRequest{repo_name: components[0].to_owned(), file_name: Some(components[1].to_owned())})}
                _ => {Err(HttpError{code: status::StatusCode::BadRequest, error: "Invalid path specified".to_owned()})}
            }
        }
        _ => {
            Err(HttpError{code: status::StatusCode::BadRequest, error: "Invalid URI specified".to_owned()})
        }
    } 
}

pub struct RestApiHandler{
    file_root: string::String
}

impl RestApiHandler {

    fn file_path(&self, uri: &hyper::uri::RequestUri) -> Result<String, String>{
        match *uri {
            hyper::uri::RequestUri::AbsolutePath(ref val) => {
                Ok(self.file_root.to_owned() + val.as_str())
            }
            _ => {
                Err("Invalid request URI".to_owned())
            }
        } 
    }

    fn process_put_req(&self, mut req: server::Request)  {
        let parsed_req = parse_request(&req.uri).unwrap();
        parsed_req.ensure_repo_exists(&self.file_root);

        let file_path = parsed_req.file_path(&self.file_root);

        let mut file = fs::File::create(&file_path).unwrap();
        let copied = io::copy(&mut req, &mut file).unwrap();
        debug!("Read {} bytes to file {}", copied, file_path);
    }

    fn process_post_req(&self, mut req: server::Request)  {
        let parsed_req = parse_request(&req.uri).unwrap();
        parsed_req.ensure_repo_exists(&self.file_root);

        let repo_path = parsed_req.repo_path(&self.file_root);
        debug!("Rebuilding metadata for repo {}", repo_path);
    }
}

impl server::Handler for RestApiHandler {
    fn handle(&self, mut req: server::Request, mut resp: server::Response) {
        match req.method {
            method::Method::Get => {
                *resp.status_mut() = hyper::status::StatusCode::Ok
            }
            method::Method::Put => {
                self.process_put_req(req);
                *resp.status_mut() = hyper::status::StatusCode::Ok;
            }
            method::Method::Post => {
                self.process_post_req(req);
                *resp.status_mut() = hyper::status::StatusCode::Ok;
            }
            _ => *resp.status_mut() = hyper::status::StatusCode::MethodNotAllowed
        }
    }
}

fn main() {
    env_logger::init().unwrap();

    let handler = RestApiHandler{file_root: "/Users/aphreet/tmp/rpm".to_owned()};

    server::Server::http("0.0.0.0:8080").unwrap().handle(handler).unwrap();
}
