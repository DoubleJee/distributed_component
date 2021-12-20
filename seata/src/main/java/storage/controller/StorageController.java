package storage.controller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import storage.service.StorageService;

@RestController
public class StorageController {

    @Autowired
    private StorageService storageService;

    @GetMapping("/deduct")
    public String deduct(){
        storageService.deduct("001",1);
        return "storage-success";
    }

}
