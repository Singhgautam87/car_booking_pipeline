terraform {
  required_providers {
    docker = {
      source = "kreuzwerker/docker"
    }
  }
}

provider "docker" {}

resource "docker_network" "pipeline_net" {
  name = "pipeline_network"
}
