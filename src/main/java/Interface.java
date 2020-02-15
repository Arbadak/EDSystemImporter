import javafx.application.Application;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class Interface extends Application {
public Group root;

    public void start(Stage primaryStage) throws Exception {

        root=new Group();
        Scene scene= new Scene(root);
        primaryStage.setScene(scene);
        primaryStage.show();

    }
}
